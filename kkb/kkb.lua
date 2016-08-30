-- author       : 刘勇跃 
-- date         : 2016-05-09

local only	= require ('only')
local scan	= require ('scan')
local utils     = require ('utils')
local json	= require ('cjson')
local socket	= require ('socket')
local supex	= require ('supex')
local mysql     = require ('mysql_pool_api')
local redis     = require ('redis_pool_api')

local HOST = '127.0.0.1'
local PORT = 4072

module('kkb', package.seeall)

--功  能:通过http协议发送参数
--参  数:cfg:ip及port信息,表格式
--       data:发送内容
--返回值:
local function http(cfg, data)
        local tcp = socket.tcp()
        if tcp == nil then
                error('load tcp failed')
                return false
        end
	tcp:settimeout(10000)
        local ret = tcp:connect(cfg['host'], cfg['port'])
	if ret == nil then
		error('connect failed!')
		return false
	end

	tcp:send(data)
	local result = tcp:receive('*a')
	tcp:close()
	return result
end

--功  能:组装http传输的内容
--参  数:json格式body
--返回值:
local function get_data( body )
	--return 'POST /' .. serv .. 'Apply.json HTTP/1.0\r\n' ..
	return 'POST /kkbprocess HTTP/1.0\r\n' ..
	'User-Agent: curl/7.33.0\r\n' ..
	string.format('Host: %s:%d\r\n', HOST, PORT) ..
	'Content-Type: application/json; charset=utf-8\r\n' ..
	--'Content-Type: application/x-www-form-urlencoded\r\n' ..
	'Connection: close\r\n' ..
	'Content-Length:' .. #body .. "\r\n" ..
	'Accept: */*\r\n\r\n' ..
	body
end

--功  能:内存分析
--参  数:可选参数类型，标签
--返回值:无
local function memory_analyse( tag )
	local msize = collectgarbage('count')
	local ldata = string.format('%s : memory use \t[%d]KB \t[%d]M', tag or '', msize, msize/1024)
	only.log('I', ldata)
end

--功  能: 检查参数 IMEI, triTime
--参  数: args
--返回值: true:参数正常;false:参数错误 
local function check_args(args)
	if not args['IMEI'] or not args['triTime'] then
		only.log('E', string.format(
			"args ERROR(IMEI:%s, triTime:%s)", 
			args['IMEI'] or "nil", args['triTime'] or "nil")
		)
		return
	else
		only.log('D','args 正常！'..scan.dump(args))
	end
	
	return true
end

--功  能: 根据触发时间计算获取历史数据的前后节点
--参  数: 触发时间戳
--返回值: 返回历史数据的前后节点时间戳
local function calTime(triTime)
	local endDay    = os.date('*t',triTime - (1 * 24 * 3600))
	local endTime   = os.time({
				year=endDay.year, 
				month=endDay.month, 
				day=endDay.day, 
				hour='23', 
				min='59', 
				sec='59'
			})                          -- 结束时间为1天前的23:59:59 
	local startDay  = os.date('*t',triTime - (1 * 24 * 3600))
	local startTime = os.time({
				year=startDay.year, 
				month=startDay.month, 
				day=startDay.day, 
				hour='00', 
				min='00', 
				sec='00'
			})                          -- 开始时间为30天前的00:00:00
	
	only.log('D',string.format(
			'calTime 完成！startTime:%s, endTime:%s',
			startTime,endTime)
			)
	return startTime, endTime
end

--功  能: 初始化文件夹，写入列名
--参  数: IMEI, triTime
--返回值: 无
local function initFile(IMEI,triTime)
	--初始化文件目录
	local dir = string.format('/home/liu/kkbfile/%s-%s',IMEI,triTime)
	local bool = assert(os.execute(string.format('mkdir -p %s', dir)))
	if bool then
		only.log('D','Directiory initialize 完成!')
	else
		only.log('E','Directiory initialize 错误!')
		return
	end		
	return dir
end

--功  能: 加密IMEI
--参  数: IMEI
--返回值: IMEI_en
local function encryption(IMEI)
	local IMEI_str = tostring(IMEI)
	--18位
	local IMEI_0   = string.sub(IMEI_str,1,5)..'0'
	local IMEI_2   = string.sub(IMEI_str,6,10)..'2'
	local IMEI_5   = string.sub(IMEI_str,11,15)..'5'
	--转置
	local IMEI_rev_0 = string.reverse(IMEI_0)
	local IMEI_rev_2 = string.reverse(IMEI_2)
	local IMEI_rev_5 = string.reverse(IMEI_5)
	--组装
	local IMEI_en = IMEI_rev_5..IMEI_rev_2..IMEI_rev_0
	
	return IMEI_en
end

--功  能: 监视执行状态码，打包文件，上传文件
--参  数: IMEI, token_cnt, dir
--返回值: 无
local function fileExecute(IMEI,token_cnt,dir)
	::RESTART::
	--监视执行状态码	
	memory_analyse('handle')
	local key = utils.str_split(dir,'/')[4]
	local ok, ret = redis.cmd('token_state_code','','hget',key,'state')
	only.log('D','state is '..scan.dump(ret))

	local token_n = tonumber(ret) or 0
	if token_n == token_cnt then
		
		--加密IMEI
		local IMEI_en = encryption(IMEI) 
		
		--新建汇总文件
		local pool_name = dir..'/'..IMEI_en..'.dat'
		local f = assert(io.open(pool_name,'a+'))
		
		--写入列名
		columns = 'CollectTime,Longitude,Latitude,Altitude,Direction,' ..
			'Speed,Mileage,A,Uid,Name,RoadType,OverSpeed,OverPercent,' ..
			'countyCode,countyName,Pname,cityName,Weather,Temperature,'
		f:write(columns,'\n') 
		
		for i = 1,token_cnt do
			--重定向
			local i_file = string.format('%s/%s.dat',dir,i)
			local f_i = assert(io.open(i_file,'r'))
			local lines = assert(f_i:read('*all'))
			f:write(lines,'\n')
			f_i:close()
		end
		f:close()

		--打包文件
		local tar_name = utils.str_split(pool_name,'.')[1]
		local ok = assert(os.execute(string.format(
			'tar -zcPf %s.tar.gz %s',tar_name,pool_name))
		)
		if not ok then
			only.log('E','打包文件失败！')
			return
		end
		
		--上传文件
--		local name = utils.str_split(name,'/')[3]
--		os.execute(string.format('bash /home/liu/data/myfile/put2kkb.sh %s.tar.gz', name))
	
		--清除对应状态码
		local ok,_ = redis.cmd('token_state_code','','DEL','key',key)
		if not ok then
			only.log('E','清除状态码失败！')	
		end

	elseif token_n < token_cnt then
		os.execute("sleep ".."300")   --等待5分钟
		goto RESTART
	elseif token_n > token_cnt then
		only.log('E','程序出错，状态码大于tokencode总数！')
	end

end

--主程序
function handle()
	only.log('D','KKB STRART!')
	local args = supex.get_our_body_table()

	only.log('E', string.format('args is %s', scan.dump(args)))

	if not check_args(args) then
		return
	end

	local t1 = socket.gettime()

	local IMEI      = tostring(args['IMEI'])
	local triTime   = tonumber(args['triTime'])      --触发时间

	--计算前后节点
	local fromTime, toTime = calTime(triTime)
	only.log('E', string.format(
		"Taskinfo IMEI %s tokenCode %s from %s to %s", 
		IMEI, tokenCode, fromTime, toTime)
	)
	--线下代码
	
	--获取tokenCode
	local select_tokenCode_sql = string.format(
		'SELECT tokenCode, startTime, endTime FROM 用户驾驶里程数据' ..
		' WHERE imei = %s AND (startTime > %d AND endTime < %d);',
		IMEI, fromTime, toTime
	)
	local ok,ret = mysql.cmd('tokencode_sql','SELECT',select_tokenCode_sql)
	if not ok or not next(ret) then
		only.log('E','select tokenCode failed!')
	end
	
--[[
	--线上代码,线上mysql表按月存储
	--取出开始结束时间的年份月份
	local st_year  = os.date(fromTime).year
	local st_month = os.date(fromTime).month
	local ed_year  = os.date(endTime).year
	local ed_month = os.date(endTime).month

	local st_ym = tostring(st_year)..tostring(st_month)
	local ed_ym = tostring(ed_year)..tostring(ed_month)
	only.log('D','st_ym is '..scan.dump(st_ym)..'ed_ym is '..scan.dump(ed_ym))
	
	--获取tokenCode	
	local select_tokenCode_sql = string.format(
		'SELECT tokenCode, startTime, endTime FROM mileageInfo%s' ..
		' WHERE imei = %s AND (startTime > %d AND endTime < %d);',
		st_ym, IMEI, fromTime, toTime
	)                                       				--开始月份
	local ok, st_ret = mysql.cmd('tokencode_sql','SELECT',select_tokenCode_sql)
	if not ok or not next(st_ret) then
		only.log('E','select start month tokenCode failed!')
	end

	local select_tokenCode_sql = string.format(
		'SELECT tokenCode, startTime, endTime FROM mileageInfo%s' ..
		' WHERE imei = %s AND (startTime > %d AND endTime < %d);',
		ed_ym, IMEI, fromTime, toTime
	)                                       				--结束月份
	local ok, ed_ret = mysql.cmd('tokencode_sql','SELECT',select_tokenCode_sql)
	if not ok or not next(ed_ret) then
		only.log('E','select end month tokenCode failed!')
	end

	local ret = {}
	
--	table.insert(ret,table.unpack(st_ret))
--	table.insert(ret,table.unpack(ed_ret))

	for i, v in ipairs(st_ret) do
		table.insert(ret,v)
	end

	for i, v in ipairs(ed_ret) do
		table.insert(ret,v)
	end
--]]
	only.log('D','ret is '..scan.dump(ret))

	--初始化目录及文件
	local dir = initFile(IMEI,triTime)
	
	--遍历tokenCode
	local token_cnt = #ret
	local n = 0
	for i, v in ipairs(ret) do
		local tokenCode = v['tokenCode']
		local startTime  = v['startTime']
		local endTime   = v['endTime']
		
		--通过http发送给开开保处理函数
		local str = {
				['i']         = i,
				['cnt']       = token_cnt,
				['IMEI']      = IMEI,
				['tokenCode'] = tokenCode,
				['startTime'] = startTime,
				['endTime']   = endTime,
				['dir']       = dir,
		}
		local body = json.encode(str)
		local data = get_data(body)
		local info = http({host = HOST, port = PORT}, data)
		n = n + 1
	end
	
	if n == token_cnt then
		only.log('I',string.format(
			'共 %d 个 tokenCode 发送完成！',token_cnt)
		)
	end

	memory_analyse('handle')
	collectgarbage('collect')

	--根据执行状态码打包上传文件，加密IMEI
	fileExecute(IMEI,token_cnt,dir)
end
