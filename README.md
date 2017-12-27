## Order book caching middleware module

Caches order book updates and makes sure the emitted updates are sensible. Some vendors emit bad data and it needs to be sanitized.

Will also inspect CTL messages and on `get_snapshot` command will fill the `order_book` result from cache if possible to save upstream resources and time.

Following checks are performed:

* If bid/ask crosses will make sure that necessary levels are cleared. Bid/ask should not logically cross when trading is enabled. That would always result in a match and filled orders. If both bid and ask side are moving closer to each other at the time the cross happens, the side with more movement will be the side that takes control. This pathological situation should not occur unless the market data emitted by the vendor is really bad.




