import pickle
import json
import numpy as np

def save(model, form=None):
    if form == None:
        form = pickle

    else:
        form == json

# data =  (np.arange(10), np.arange(12).reshape(3,4))
#
# f = open(r'E:\IdeaWorkSpace\EnergyManagement4\EnergyManagement\python\python\MOPSO\data\modelpickle', 'wb')
# pickle.dump(data, f)

a = pickle.load(open(r'E:\IdeaWorkSpace\EnergyManagement4\EnergyManagement\python\python\MOPSO\data\model.pickle','rb'))
print(a[0])
