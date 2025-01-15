```python
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


data_path = "/content/part-00000-2fe84245-ebbe-45f5-b536-15a704d3bd9f-c000.csv"  # Replace with the correct path
df = pd.read_csv(data_path)

top_cities = df.nlargest(20, "startups_count")  # Top 20 cities with most startups


plt.figure(figsize=(14, 8))
sns.barplot(
    data=top_cities,
    y="Startup_City",
    x="startups_count",
    hue="Startup_State",
    dodge=False,
    palette="viridis"
)
plt.title("Top Cities by Startup Count (2015-2024)")
plt.xlabel("Number of Startups")
plt.ylabel("City")
plt.legend(title="State")
plt.tight_layout()
plt.show()


heatmap_data = df.pivot_table(index="Startup_City", columns="Startup_State", values="startups_count", fill_value=0)

plt.figure(figsize=(16, 12))
sns.heatmap(
    heatmap_data,
    cmap="coolwarm",
    annot=True,
    fmt="d",
    linewidths=0.5,
    linecolor="gray"
)
plt.title("Startup Distribution by City and State (2015-2024)")
plt.xlabel("State")
```

![Alt_text](../img/su_univeridades.pns)
