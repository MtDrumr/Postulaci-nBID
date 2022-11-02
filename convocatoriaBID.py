# PDF read at 8:30 am

# Work started at 9:10 am

# Work finished at 2:45 pm (delayed because of midterm exams week)

# Coments ended at 3:10 pm


# Import the libraries used
import pandas as pd
import numpy as np

# Read the csv
bid=pd.read_csv("scp-1205.csv")

# Drop the column names as the first row
bid2=pd.DataFrame(np.vstack([bid.columns, bid]))

# Change the name of the column names
dict1={
 0:"countyname",
 1:"state",
 2:"contract",
 3:"healthplanname",
 4:"typeofplan",
 5:"countyssa",
 6:"elegibles",
 7:"enrollees",
 8:"penetration",
 9:"ABrate"
}

bid3=bid2.rename(columns=dict1)

# Change the empty spaces as zeros
def zeros(x):
    if x == "  ":
        return 0
    elif x == ".":
        return 0
    elif x == ". ":
        return 0
    else:
        return x
    
bid4=bid3.applymap(lambda x: zeros(x))

# Remove the spaces of the following columns
for x in ["countyname", "state", "contract", "healthplanname", "typeofplan"]:
    bid4[x]=bid4[x].apply(lambda x: str(x)[:-1])

# Only consider data outside Puerto Rico and Guam
bid5=bid4.loc[bid4["state"].apply(lambda x: x in ["PR","GU"])==False]

# Consider only standardized data
def numeros(i):
    lista_c=[]
    for x in str(i):
        lista_c.append(x in [str(x) for x in range(10)])
    if False in lista_c:
        return 0
    else:
        return sum(lista_c)
    
bid6=bid5.loc[bid5["countyssa"].apply(lambda x: numeros(x))!=0].copy()

# Change the type of the values to make the operations
for x in ["countyssa","elegibles","enrollees"]:
    bid6[x]=bid6[x].apply(lambda x: int(x))
    
for x in ["penetration","ABrate"]:
    bid6[x]=bid6[x].apply(lambda x: float(x))
    
# Create the data grouped by each county
base=bid6[["countyname","state","countyssa"]].drop_duplicates().reset_index(drop=True)
np1=bid6.loc[bid6["enrollees"]>10].groupby("countyname").agg("count").reset_index(level=0)[["countyname","enrollees"]].rename(columns={"enrollees":"numberofplans1"})
np2=bid6.loc[bid6["penetration"]>0.5].groupby("countyname").agg("count").reset_index(level=0)[["countyname","enrollees"]].rename(columns={"enrollees":"numberofplans2"})
np3=bid6.groupby("countyname").sum().reset_index(level=0)[["countyname","elegibles","enrollees"]]

# Merge the generated DataFrames
merge1=base.merge(np1, how='left', on='countyname')
merge2=merge1.merge(np2,how="left",on="countyname")
merge3=merge2.merge(np3,how="left",on="countyname").rename(columns={"enrollees":"totalenrollees"})
merge3["totalpenetration"]=merge3["totalenrollees"]/merge3["elegibles"]

# Make the final cleaning to the database
def nan(x):
    try:
        return int(x)
    except:
        return 0

merge3["numberofplans1"]=merge3["numberofplans1"].apply(lambda x: nan(x))
merge3["numberofplans2"]=merge3["numberofplans2"].apply(lambda x: nan(x))

# Store the final database
final=merge3.sort_values(by=["state","countyname"]).replace(np.nan,0)
final.set_index("countyname").to_excel("scp-1205.xlsx")
