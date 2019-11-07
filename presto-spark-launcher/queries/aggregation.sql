SELECT partkey, count(*) c
FROM tpch.tiny.lineitem
WHERE partkey % 10 = 1
GROUP BY partkey
HAVING count(*) = 42