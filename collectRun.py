


array = [{"file1":"name1","file2":"name2"}]

print(type(array))
if type(array) == list:
    array = array[0]
    print(array)
    print(type(array))
