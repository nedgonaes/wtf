import hyperclient
c = hyperclient.Client('127.0.0.1', 1981)
c.add_space('''
space wtf_master 
key path_range 
attributes map(int, string) blocks 
subspace blocks 
create 8 partitions
tolerate 2 failures
''')
