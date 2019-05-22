from input import *

input = InputMongoDB('robotic')

data_list = [
                {
                    'className': 'Robot',
                    'name': 'aibo',
                    'data': {'position': [1, 1]},
                    'references': {'camera': ['+camera2']}
                },
                {
                    'className': 'Camera',
                    'name': 'camera2',
                    'data': {'image': 'IMAGE3'}
                }
            ]

input.add_from_list(data_list)

try:
    input.send_data()
    print("SUCCESS!")
except:
    print("ERROR!")

# ================================================================================

data = {
    'data': {'position': [1, 2, 3]},
    'params': {'dim': [1, 2], 'id': 10.0},
    'references': {'camera': ['+camera1'],'temperature': '+temp1'}
}
input.add_item('Robot', 'geko', **data)

data = {
    'data': {'position': [2, 2, 1]},
    'params': {'dim': [5, 1]},
    'references': {'camera': '+camera1'}
}
input.add_item('Robot', 'waldo', **data)

data = {
    'data': {'image': 'IMAGE'},
    'params': {'intrinsic_params': [100.0, 200, 0, 13.0, 15.0, 5.0]},
    'references': {'robot': '+geko'}
}
input.add_item('Camera', 'camera1', **data)

data = {
    'data': {'temperature': 20.5}
}
input.add_item('Temperature', 'temp1', **data)

try:
    input.send_data()
    print("SUCCESS!")
except:
    print("ERROR!")

# ================================================================================

data = {
    'data': {'position': [4,5,6]},
    'params': {'model': 'burger'},
    'references': {'camera': ['+camera1']}
}
input.add_item('Robot', 'turtlebot', **data)

data = {
    'data': {'position': [8,5,6]},
    'params': {'id': 20.0}
}
input.add_item('Robot', 'geko', **data)

data = {
    'data': {'image': 'IMAGE2'},
}
input.add_item('Camera', 'camera1', **data)

data = {
    'data': {'temperature': 20.5},
    'params': {'temp_min': -50.0}
}
input.add_item('Temperature', 'temp1', **data)

try:
    input.send_data()
    print("SUCCESS!")
except:
    print("ERROR!")
