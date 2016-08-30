-- author       : 刘勇跃 
-- date         : 2016-05-09

local only			= require ('only')
local scan			= require ('scan')
local utils                     = require ('utils')
local socket			= require ('socket')
local supex			= require ('supex')
local redis                     = require ('redis_pool_api')

local TEN_MINUTES	        = 600	--  tsdb数据存储以10分钟为单位
local FRAME_LEN			= 12	--  每一个frame有FRAME_LEN个10分钟的长度
local FRAME_TIME		= FRAME_LEN * TEN_MINUTES

local GPSFrameModule            = require('gps_frame')
local GPSFrame                  = GPSFrameModule.GPSFrame

module('kkbprocess', package.seeall)

--功  能:内存分析
local function memory_analyse( tag )
	local msize = collectgarbage('count')
	local ldata = string.format('%s : memory use \t[%d]KB \t[%d]M', tag or '', msize, msize/1024)
	print(ldata)
	only.log('I', ldata)
end

--功  能: 检查参数
--参  数: args
--返回值: true:参数正常;false:参数错误 
local function check_args(args)
	if not args['i'] then
		only.log('E', string.format(
			'args ERROR tokenCode I:%s', args['i'] or 'nil'))
		return
	elseif  not args['cnt'] then
		only.log('E', string.format(
			'args ERROR tokenCode cnt:%s', args['cnt'] or 'nil'))
		return
	elseif not args['IMEI'] then
		only.log('E', string.format(
			'args ERROR IMEI:%s', args['imei'] or 'nil'))
		return
	elseif not args['tokenCode'] then
		only.log('E', string.format(
			'args ERROR tokenCode:%s', args['tokenCode'] or 'nil'))
		return
	elseif  not args['startTime'] then
		only.log('E', string.format(
			'args ERROR startTime:%s', args['startTime'] or 'nil'))
		return
	elseif not args['endTime'] then
		only.log('E', string.format(
			'args ERROR endTime:%s', args['endTime'] or 'nil'))
		return
	elseif not args['dir'] then
		only.log('E', string.format(
			'args ERROR dir:%s', args['dir'] or 'nil'))
		return
	else
		only.log('D','args 正确!'..scan.dump(args))
	end
	return true
end

--主函数
function handle()
	only.log('D','KKBProcess START!')
	local args = supex.get_our_body_table()

	if not check_args(args) then
		return
	end

	local t1 = socket.gettime()

	local i         = args['i']
	local token_cnt = args['cnt']
	local IMEI      = args['IMEI']
	local tokenCode = args['tokenCode']
	local startTime = args['startTime']
	local endTime   = args['endTime']
	local dir       = args['dir']

	local st_time = tonumber(startTime)
	local ed_time = tonumber(endTime)
	local frame_start_time = st_time
	local frame_array = {}
	local frame_idx = 1

	--循环计算每个frame数据
	--以两个小时时长作为一个frame单位进行计算
	repeat
		--检查开始结束时间，错误直接跳过
		if startTime > endTime then                                           
        		only.log('E', string.format(
				'ERROR todo task IMEI %s tokenCode %s from %s to %s', 
        	                IMEI, tokenCode, startTime, endTime)
			)
			break    --跳过  
        	end

		local frame_end_time = (math.floor(frame_start_time / FRAME_TIME)  + 1) * (FRAME_TIME) - 1
		if frame_end_time >= ed_time then	--最后一个frame
			frame_end_time = ed_time
		end

		local gps_frame = GPSFrame:new(IMEI,tokenCode,frame_start_time,frame_end_time,i,token_cnt,dir)
		frame_array[frame_idx] = gps_frame
		only.log('D', string.format(
			'frame idx:%s, start:%s, end:%s', 
			frame_idx, frame_start_time, frame_end_time)
		)
		
		--处理数据
		gps_frame:process()

		frame_start_time = frame_end_time + 1
		frame_idx = frame_idx + 1

		memory_analyse('frame repeat')
		collectgarbage('collect')
	until frame_end_time >= ed_time

	local t_end = socket.gettime()

	memory_analyse(string.format('%s process  end!',i))
	collectgarbage('collect')

	--记录执行状态
	local key = utils.str_split(dir,'/')[4]
	local _,exist = redis.cmd('token_state_code','','hexists',key,'state')
	only.log('D','state exist is '..scan.dump(exist))
	
	if exist == false then	
		local ok,_ = redis.cmd('token_state_code','','hset',key,'state',1)
	elseif exist == true then
		local ok,token_n = redis.cmd('token_state_code','','hget',key,'state')
		if not ok then
			only.log('D','redis 获取状态码失败！')
		end
		
		token_n = token_n + 1
		local ok,_ = redis.cmd('token_state_code','','hset',key,'state',token_n)
	end

end
