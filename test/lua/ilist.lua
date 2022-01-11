
function new(record, binname, ...)
  local l  = list();
  --local il = list.indexed(l, 'iname');
  for k, v in pairs(arg) do if (k ~= "n") then list.append(il, v); end end
  record[binname] = il;
  aerospike:update(record);
  return il;
end

function add(record, binname, ...)
  local il = record[binname];
  if (il == nil) then return nil; end
  for k, v in pairs(arg) do if (k ~= "n") then list.append(il, v); end end
  record[binname] = il;
  aerospike:update(record);
  return il;
end
