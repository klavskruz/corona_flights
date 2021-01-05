import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

plt.close("all")

# Create your connection.

DATABASE_LOCATION = "corona_flight_data.sqlite"

cnx = sqlite3.connect(DATABASE_LOCATION)

df = pd.read_sql_query("SELECT * FROM Amsterdam", cnx)
df = df[df['Total'] > 0]
df.plot(x = "Active", y = "Price-Average")
plt.show()
print(df["Price-Average"].corr(df["Total"]))
