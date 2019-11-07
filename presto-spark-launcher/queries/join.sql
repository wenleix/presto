SELECT l.orderkey, l.linenumber, o.orderstatus
FROM lineitem l
JOIN orders o
ON l.orderkey = o.orderkey
WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 AND o.orderstatus = 'O'