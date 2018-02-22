local require = require
local tools = require("wtf.core.tools")
local Storage = require("wtf.core.classes.storage")

local mongol = require("wtf.fork.resty-mongol")

local _M = Storage:extend()
_M.name = "mongodb"

function _M:init(...)
  local err = tools.error
  local user = self:get_optional_parameter('user') or nil
  local password = self:get_optional_parameter('password') or nil
  local host = self:get_optional_parameter('host') or 'localhost'
  local port = self:get_optional_parameter('port') or 27017
  local auth_db = self:get_optional_parameter('auth_db') or nil
  
  local db = self:get_mandatory_parameter('db')
  local collection = self:get_mandatory_parameter('collection')
  
  local name = self:get_optional_parameter('name') or self.name
  
  self.handler = {}

  local ok, e
  local db_handler, auth_db_handler
  local conn = mongol()
  
  if host ~= nil then
    if port ~= nil then
      ok, e = conn:connect(host, port)
    else
      ok, e = conn:connect(host)
    end
    
    if ok ~= nil then
      conn:set_timeout(60*60*1000)
    else
      err("Error when connecting to storage '"..name.."': "..e)
    end
  else
    err("Mandatory option 'host' is missing when configuring storage '"..name.."'")
  end
  
  if user ~= nil and password ~= nil then
    
    if auth_db ~= nil then
      auth_db_handler = conn:new_db_handle(auth_db)
    else
      auth_db_handler = conn:new_db_handle(db)
    end
    
    if auth_db_handler == nil then
      err("Error when getting authentication db for storage '"..name.."'")
    end
    
    ok,e = auth_db_handler:auth(user, password)
    if ok == nil then
      err("Error when logging to storage '"..name.."': "..e)
    end
  end
  
  db_handler = conn:new_db_handle(db)
  
  if db_handler == nil then
    err("Error when getting db for storage '"..name.."'")
  end
  
  self.handler = db_handler:get_col(collection)
    
  if self.handler == nil then
    err("Error when getting collection for storage '"..name.."'")
  end

  return self
end

function _M:get(key)
  local err = tools.error
  local name = self:get_optional_parameter('name') or self.name
  local key_field = self:get_optional_parameter('key_field') or "key"
  local value_field = self:get_optional_parameter('value_field') or "value"
  local search_condition = {}
  local doc = {}
  local e
  
  if self.handler == nil then err("Handler is nil when getting data from storage '"..name.."'") end
  
  if key_field ~= nil then
    if key ~= nil then
      search_condition[key_field] = key
      doc, e = self.handler:find_one(search_condition) 
    else
      err("Cannot get value for empty key from storage '"..name.."'")
    end
  else
    err("Mandatory option 'key_field' is missing when getting data from storage '"..name.."'")
  end
  
  if doc ~= nil and doc[value_field] ~= nil then
    return doc[value_field]
  else
    return nil
  end
end

function _M:set(key, value)
  local err = tools.error
  local name = self:get_optional_parameter('name') or self.name
  local key_field = self:get_optional_parameter('key_field') or "key"
  local value_field = self:get_optional_parameter('value_field') or "value"
  local search_condition = {}
  local new_data = {}
  local num, e

  if self.handler == nil then err("Handler is nil when getting data from storage '"..name.."'") end

  if key_field ~= nil then
    if key ~= nil then
      if value == nil then value = "" end
      search_condition[key_field] = key
      new_data[key_field] = key
      new_data[value_field] = value
      num, e = self.handler:update(search_condition, new_data, 1) 
    else
      err("Cannot set value for empty key from storage '"..name.."'")
    end
  else
    err("Mandatory option 'key_field' is missing when getting data from storage '"..name.."'")
  end
  
  return self
end

function _M:del(key)
  local err = tools.error
  local name = self:get_optional_parameter('name') or self.name
  local key_field = self:get_optional_parameter('key_field') or "key"
  local search_condition = {}
  local num, e
  
  if self.handler == nil then err("Handler is nil when deleting data from storage '"..name.."'") end

  if key_field ~= nil then
    if key ~= nil then
      search_condition[key_field] = key 
      num, e = self.handler:remove(search_condition, 1) 
    else
      err("Cannot delete value for empty key from storage '"..name.."'")
    end
  else
    err("Mandatory option 'key_field' is missing when deleting data from storage '"..name.."'")
  end
  
  return self
end

return _M
