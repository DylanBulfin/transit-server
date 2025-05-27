Next tries to improve performance:

## Multiple servers
The plan is to have 2 servers:
1. This one recieves requests from Caddy. If the response is cached, it sends the cached response. Otherwise it forwards the request to server 2
2. This one recieves and processes non-cached requests

### Considerations
Need some way to ensure the two servers are synced. As part of this, the Schedule service could have one additional method, GetServerStartTime, that returns a u32. With each new request the inbetween server will check this value, and if it has changed it will undo all its caching logic. 
