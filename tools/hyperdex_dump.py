import hyperclient
c = hyperclient.Client('127.0.0.1', 1981)
print c.get('wtf','home')
