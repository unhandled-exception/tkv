#!/usr/bin/env tarantool

local json = require('json')
local log = require('log')

HTTP_HOST = 'localhost'
HTTP_PORT = '8080'
MAX_REQUESTS_PER_SECONDS = 10


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


box.cfg{
    memtx_dir='./data',
    wal_dir='./data'
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

local server = require('http.server').new(
    HTTP_HOST,
    HTTP_PORT,
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
    build_rate_limiter_middleware(MAX_REQUESTS_PER_SECONDS),
    {
        path = '/kv.*',
        method = 'ANY'
    }
)

server:start()
