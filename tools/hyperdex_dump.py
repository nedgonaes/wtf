import hyperclient
c = hyperclient.Client('127.0.0.1', 1981)
for x in c.search('wtf',{}):
    print(x)
#print c.get('wtf','home')
