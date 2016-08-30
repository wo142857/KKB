--文件名称: gps_frame.lua
--创建者  : 刘勇跃
--创建时间: 2016-05-09
--文件描述: 处理每个frame的途径(frame即时长 相同的一个片段)

local only			= require ('only')
local utils			= require ('utils')
local scan			= require ('scan')
local lua_decoder		= require ('libluadecoder')
local link			= require ('link')
local RoadRecordModule 		= require('road_record')
local RoadRecord		= RoadRecordModule.RoadRecord

local TEN_MINUTES               = 600
--不同字段在tsearch接口返回数据中的位置
local G_TSEARCH_IDX_TIME	= 1
local G_TSEARCH_IDX_CRETIME     = 2
local G_TSEARCH_IDX_IMEI        = 3
local G_TSEARCH_IDX_LON		= 7
local G_TSEARCH_IDX_LAT		= 8
local G_TSEARCH_IDX_ALT         = 9
local G_TSEARCH_IDX_DIR		= 10
local G_TSEARCH_IDX_SPEED	= 11
local G_TSEARCH_IDX_TOKENCODE	= 12

module('gps_frame', package.seeall)

GPSFrame = {
	IMEI,
	tokenCode,
	frameStart,	--frame开始时间
	frameEnd,	--frame结束时间
	startPoint,	--当前frame开始GPS点信息
	endPoint,	--当前frame结束GPS点信息
	recordCount,	--当前frame中record个数
	recordSet,
	i,              --第n个tokenode
	token_cnt,      --tokenode总数
	dir             --初始化文件目录
}

function GPSFrame:new(IMEI,tokenCode,startTime,endTime,i,token_cnt,dir)
	local self = {
	}

	setmetatable(self, GPSFrame)
	GPSFrame.__index = GPSFrame

	--初始化原始属性
	self['IMEI']        = IMEI
	self['tokenCode']   = tokenCode
	self['frameStart']  = startTime
	self['frameEnd']    = endTime
	self['recordCount'] = 0
	self['recordSet']   = {}
	self['i']           = i
	self['token_cnt']   = tokenCode
	self['dir']         = dir

	return self
end 

--功  能: 调用tsearch接口获取gps数据
--参  数: tsearch_api	: 获取正常数据或者补偿数据api name
--	  IMEI		: IMEI	
--	  st_time	: tsearch查询开始时间
--	  ed_time	: tsearch查询结束时间
--返回值: frame_data: frame时间段内正常或补传数据
function GPSFrame:getGPSData(tsearch_api, IMEI, st_time, ed_time)
	local body_info = {imei = IMEI, startTime = st_time, endTime = ed_time}
	local serv = link['OWN_DIED']['http']['tsearchapi/v2/getgps']
	local body = utils.gen_url(body_info)
	local api_name = 'tsearchapi/v2/' .. tsearch_api

	local body_data = utils.compose_http_json_request(serv, api_name, nil, body)

	local ok, ret = supex.http(serv['host'], serv['port'], body_data, #body_data)
	if not ok or not ret then 
		only.log('E', 'getGPSData is ' .. scan.dump(ret))
		return nil
	end
	-->获取RESULT后的数据	
	only.log('D', '%s, length:%s', tsearch_api, #ret)
	local data = utils.parse_api_result(ret, tsearch_api)
	if not data then
		return {}
	end
	if #data == 0 then return {} end
	--data中，偶数下标的元素才是gps数据,去掉奇数元素
	for k,_ in ipairs (data) do
		table.remove(data, k)
	end

	--使用tsearch解码格式解析数据
	local frame_data = lua_decoder.decode(#data, data)
	if not frame_data then
		return {}
	end

	return frame_data
end

--功  能: 获取gps数据
--返回值: frame_gps_data: frame时间段内正常gps数据
--	  frame_ext_data: frame时间段内补传gps数据
function GPSFrame:getData()
	local frame_gps_data
	local frame_ext_data

	if self['frameStart'] > self['frameEnd'] then
		only.log('E', string.format(
			'frame time ERROR, start[%s] > end[%s]', 
			self['frameStart'], self['frameEnd'])
		)
		return {}, {} 
	end

	--获取正常数据
	frame_gps_data = self:getGPSData('getgps', self['IMEI'], self['frameStart'], self['frameEnd'])
	--获取补传数据
	frame_ext_data = self:getGPSData('getExtGps', self['IMEI'], self['frameStart'], self['frameEnd'])

	return frame_gps_data, frame_ext_data
end

--将gps数组转换为 gpstime为key的kv形式
function GPSFrame:arrayToKV(dest_data, src_data, isExtra)
	if not src_data or not dest_data then
		return
	end

	for i,val in ipairs(src_data) do
		repeat
			local gps_point = {
				['time']      = tonumber(val[G_TSEARCH_IDX_TIME]),
				['create']    = tonumber(val[G_TSEARCH_IDX_CRETIME]),
				['lon']       = tonumber(val[G_TSEARCH_IDX_LON]),
				['lat']       = tonumber(val[G_TSEARCH_IDX_LAT]),
				['alt']       = tonumber(val[G_TSEARCH_IDX_ALT]),
				['speed']     = tonumber(val[G_TSEARCH_IDX_SPEED]),
				['dir']       = tonumber(val[G_TSEARCH_IDX_DIR]),
				['tokenCode'] = val[G_TSEARCH_IDX_TOKENCODE],
				['isExtra']   = isExtra,
			}
			--tokenCode不相同，直接抛弃掉
			if gps_point['tokenCode'] ~= self['tokenCode'] then
				break 		--continue
			end

			dest_data[gps_point['time']] = gps_point

			--开始gps点
			if not self['startPoint'] then
				self['startPoint'] = gps_point
			elseif self['startPoint']['time'] > gps_point['time'] then
				self['startPoint'] = gps_point
			end

			--结束gps点
			if not self['endPoint'] then
				self['endPoint'] = gps_point
			elseif self['endPoint']['time'] < gps_point['time'] then
				self['endPoint'] = gps_point
			end
		until true
	end
end

--功  能: 合并正常数据与补传数据
--参  数: gps_data: 正常数据
--	  ext_data: 补传数据
--返回值: merge_data: 合并后的gps数据
function GPSFrame:merGPSData(gps_data, ext_data)
	local merge_data = {}

	self:arrayToKV(merge_data, ext_data, true)	--先转换补传数据	
	self:arrayToKV(merge_data, gps_data, false)	--后转换正常数据，覆盖补传数据进行去重。

	return merge_data
end

local function direction_sub(dir1, dir2)
	local angle = math.abs(dir1 - dir2)
	return (angle <= 180) and angle or (360 - angle)
end

--功  能: 计算两点间的里程及加速度
--参  数: prev_gps: 第一个gps点
--	  next_gps: 第二个gps点
--返回值: mileage:  计算所得有效里程
--	  acceleration: 计算所得加速度
function GPSFrame:calcMiles(prev_gps, next_gps)
	local mileage, acceleration = prev_gps[13], 0  --传进来上一个点的里程
	local time1 = prev_gps['time']
	local speed1 = (prev_gps['speed'] ~= -1) and prev_gps['speed'] or 0
	local time2 = next_gps['time']
	local speed2 = (next_gps['speed'] ~= -1) and next_gps['speed'] or 0

	if time1 >= time2 then
		return 0, 0
	elseif (time2 - time2) > 5 then	--时间间隔大于5秒 不计算
		return 0, 0
	else
		local a = ((speed2 - speed1) * (1000 / 3600)) / (time2 - time1)  --加速度
		local s = ((speed1 + speed2) / 2 ) * ( 1000 / 3600 ) * (time2 - time1)
		mileage	= tonumber(string.format('%.1f',s)) + mileage
		acceleration = string.format('%.1f',a)	
		return math.floor(mileage), acceleration
	end
end

--计算途径
function GPSFrame:calcPath(data)
	local st_idx = self['startPoint']['time']
	local ed_idx = self['endPoint']['time']
	local roadrecord
	local gps_point
	local last_point
	local last_record = {}
	local premileage = 0

	for i=st_idx, ed_idx do
		repeat
			gps_point = data[i]
			if not gps_point then
				break         --continue
			end	
--			only.log('D','gps_point is '..scan.dump(gps_point))
			
			roadrecord = RoadRecord:new()

			--获取道路信息
			roadrecord:getRoadInfo(self['IMEI'], self['tokenCode'], gps_point, last_record)
	
			--计算里程、加速度
			if not last_point then
				last_point = gps_point
				last_point[13] = premileage
			else
				local mileage, acceleration = self:calcMiles(last_point, gps_point)
				roadrecord:addMileage(mileage)
				roadrecord:addAcceleration(acceleration)
				last_point = gps_point
				last_point[13] = mileage
			end

			self['recordCount'] = self['recordCount'] + 1
			last_record = roadrecord
			table.insert(self['recordSet'], roadrecord)
			
		until true
	end
end

--功  能: 将recordSet写入文件
--
function GPSFrame:writeToFile(dir,i)
	local fileName = string.format('%s/%s.dat',dir,i)
	local f = assert(io.open(fileName, 'w'))
	for i, line in ipairs(self['recordSet']) do
		local tmp = {}
		tmp = {
			[1]  = line['CollectTime'],
			[2]  = line['Longitude'],
			[3]  = line['Latitude'],
			[4]  = line['Altitude'],
			[5]  = line['Direction'],
			[6]  = line['Speed'],
			[7]  = line['Mileage'] or 0,
			[8]  = line['A'] or 0.0,
			[9]  = line['Uid'],
			[10] = line['Name'] or 'null',
			[11] = line['RoadType'] or 'null',
			[12] = line['OverSpeed'] or 'null',
			[13] = line['OverPercent'] or 'null',
			[14] = line['countyName'] or 'null',
			[15] = line['PName'] or 'null',
			[16] = line['cityName'] or 'null',
			[17] = line['Weather'] or 'null',
			[18] = line['Temperature'] or 'null'
		}
		
		for _, v in ipairs(tmp) do
			f:write(tostring(v),',')
		end
		f:write('\n')
	end
	f:close()
end

--开始处理当前frame
function GPSFrame:process()

	local t1 = socket.gettime()
	local gps_data, ext_data = self:getData()
	local t2 = socket.gettime()
	only.log('D', 'GPSFrame:process getData time:%s', t2 - t1)

	--合并正常数据与补传数据
	local data = self:merGPSData(gps_data, ext_data)
	local t3 = socket.gettime()
	only.log('D', 'GPSFrame:process merGPSData time:%s', t3 - t2)
	if not next(data) then
		only.log('W', string.format(
			'merge gps data is none! IMEI:%s, tokenCode:%s', 
			self['IMEI'], self['tokenCode'])
		)
		return
	end
	if (self['endPoint']['time'] - self['startPoint']['time']) > (48+1) * 3600 then
		only.log('E', string.format(
			'time over two days,IMEI:%s, tokenCode:%s, start:%s, end:%s',
			self['IMEI'], self['tokenCode'], self['startPoint']['time'], 
			self['endPoint']['time'])
		)
		return
	end
	local t4 = socket.gettime()
	only.log('D', 'GPSFrame:process filterGPSData time:%s', t4 - t3)
	--计算
	self:calcPath(data)
	local t5 = socket.gettime()
	only.log('D', 'GPSFrame:process calcPath time:%s', t5 - t4)
	
	--写入文件
	self:writeToFile(self['dir'],self['i'])
	local t6 = socket.gettime()
	only.log('D','GPSFrame:process writeToFile time:%s', t6 - t5)
end

