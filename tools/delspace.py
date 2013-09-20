import hyperclient
c = hyperclient.Client('127.0.0.1', 1981)
c.rm_space('wtf')
