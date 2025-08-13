import pandas as pd

list = {
    'calories': [420,150,236],
    'sugarcoat': [50,25,100]
}

myvar = pd.DataFrame(list, index= ('day', 'day2','day3'))

print(myvar.loc['day2'])