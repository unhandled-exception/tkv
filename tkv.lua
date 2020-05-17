#!/usr/bin/env tarantool

local argparse = require('argparse')
local json = require('json')
local log = require('log')
local fio = require('fio')

DEFAULT_HTTP_HOST = 'localhost'
DEFAULT_HTTP_PORT = '8080'
DEFAULT_MAX_REQUESTS_PER_SECONDS = 25
DEFAULT_DATA_DIR = './data'
DEFAULT_LOG_FILE = ''


function on_index(req)
    return {
        status = 200,
        headers = {
            ['Content-Type'] = 'text:html; charset=utf-8'
        },
        body = [[
            <html>
                <body>
                    <h1>The Tarantool key-value storage!</h1>
                </body>
            </html>
        ]]
    }

end


function make_json_response(status, body)
    if body == nil then
        body = ''
    else
        body = json.encode(body)
    end

    return {
        status = status,
        headers = {['content-type'] = 'application/json'},
        body = body
    }
end


function on_create_key(req)
    local ok, data = pcall(req.json, req)
    if not ok or data.key == nil or type(data.key) ~= 'string' or data.value == nil then
        return make_json_response(400, {error = 'Bad request'})
    end

    local status = 200
    value = data.value
    local ok, error = pcall(box.space.kv.insert, box.space.kv, {data.key, value})
    if not ok then
        status = 409
        log.info('Create key. Key already exists key=' .. data.key)
    else
        log.info('Create key. Insert pair: key=' .. data.key .. ' value=' .. json.encode(value))
    end

    return make_json_response(status, data)
end


function on_get_value(req)
    local result = nil
    local status = 200
    local key = req:stash('key')

    if not key then
        return make_json_response(400, {error = 'Bad request'})
    end

    local data = box.space.kv:get(key)
    if data then
        result = data.value
        log.info('Get value: Return key=' .. key .. ' value=' .. json.encode(data.value))
    else
        status = 404
        result = {error = 'Key not found'}
        log.info('Get missing key=' .. key)
    end

    return make_json_response(status, result)
end


function on_delete_key(req)
    local result = nil
    local status = 200
    local key = req:stash('key')

    if not key then
        return make_json_response(400, {error = 'Bad request'})
    end

    local data = box.space.kv:get(key)
    if data then
        box.space.kv:delete(key)
        result = {status = 'ok'}
        log.info('Delete key. Delete key=' .. key)
    else
        status = 404
        result = {error = 'Key not found'}
        log.info('Delete key. Get missing key=' .. key)
    end
    return make_json_response(status, result)
end


function on_update_value(req)
    local result = nil
    local status = 200
    local key = req:stash('key')

    local ok, data = pcall(req.json, req)
    if not key or not ok or data.value == nil then
        return make_json_response(400, {error = 'Bad request'})
    end

    box.begin()
    local ok = box.space.kv:get(key)
    log.error(ok)
    if ok then
        value = data.value
        box.space.kv:replace{key, value}
        result = {status = 'ok'}
        log.info('Update value. key=' .. key .. ' value=' .. json.encode(value))
    else
        status = 404
        result = {error = 'Key not found'}
        log.info('Update value. Get missing key=' .. key)
    end
    box.commit()

    return make_json_response(status, result)
end


function build_rate_limiter_middleware(max_requests_per_second)
    local last_time = os.time()
    local requests = 0

    return function(req)
        local cur_time = os.time()
        if cur_time ~= last_time then
            last_time = cur_time
            requests = 1
            return req:next()
        elseif max_requests_per_second > requests then
            requests = requests + 1
            return req:next()
        end
        log.error('The limit on the number of requests per second was triggered')
        return make_json_response(429, {error = 'Too many requests'})
    end
end


function init_box(data_dir, log_filename)
    fio.mktree(data_dir)

    box.cfg{
        memtx_dir = data_dir,
        wal_dir = data_dir,
        log = log_filename
    }

    box.once(
        'bootstrap_db',
        function ()
            local kvs = box.schema.space.create('kv')
            kvs:format{
                {name = 'key', type = 'string'},
                {name = 'value'}
            }
            kvs:create_index('pk', {type = 'hash', parts = {'key'}})
        end
    )
end


function init_webserver(host, port, rate_limit)
    local server = require('http.server').new(
        host,
        port,
        {
            display_errors = false
        }
    )
    local router = require('http.router').new()
    server:set_router(router)
    router:route({path = '/'}, on_index)
    router:route({path = '/kv', method='POST'}, on_create_key)
    router:route({path = '/kv/:key', method='GET'}, on_get_value)
    router:route({path = '/kv/:key', method='PUT'}, on_update_value)
    router:route({path = '/kv/:key', method='DELETE'}, on_delete_key)
    local ok = router:use(
        build_rate_limiter_middleware(rate_limit),
        {
            path = '/kv.*',
            method = 'ANY'
        }
    )
    return server
end


local parser = argparse{
    description = 'Simple key-value storage',
    epilog = 'For more info, see https://github.com/unhandled-exception/tkv'
}
parser:option{name = '-d --data-dir',   default = DEFAULT_DATA_DIR,                 description = 'Tarantool data dir'}
parser:option{name = '-l --log-file',   default = DEFAULT_LOG_FILE,                 description = 'Tarantool log file name (if empty write to stdout)'}
parser:option{name = '-h --host',       default = DEFAULT_HTTP_HOST,                description = 'HTTP host'}
parser:option{name = '-p --port',       default = DEFAULT_HTTP_PORT,                description = 'HTTP port'}
parser:option{name = '-r --rate-limit', default = DEFAULT_MAX_REQUESTS_PER_SECONDS, description = 'Max requests per seconds'}
local args = parser:parse()

init_box(args.data_dir, args.log_file)
local server = init_webserver(args.host, args.port, args.rate_limit)
server:start()
