# WWCP

wwcp is a webhook relay allowing you to receive webhooks even when you
don't have a direct inbound connection from the internet (or when you
are only partially connected to the internet).

It works by storing your inbound hooks on a machine that is on the
internet ([GAE][gae]) and then providing a client to poll for those
hooks at your leisure.

[gae]: https://appengine.google.com/
