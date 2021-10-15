select hex(w.id), w.namespace, w.name, w.last_modified,
wec.entity_cache_last_updated, wec.error_message
from WORKSPACE w, WORKSPACE_ENTITY_CACHE wec
where wec.workspace_id = w.id
and wec.entity_cache_last_updated = '1970-01-01 00:00:01.000000';
