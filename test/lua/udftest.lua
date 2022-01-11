function f1(record)
  local bn1   = __Args['bn1'];
  local bn2   = __Args['bn2'];
  local inner = {bn3 = record.bn1};
  local nest  = {bn1 = bn1; inner = inner};
  local ret   = {size = record[bn1] + string.len(record[bn2]),
                 bn2  = record.bn2,
                 nest = nest};
  return ret;
end
