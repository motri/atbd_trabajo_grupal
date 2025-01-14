## Script the python para generar el grafico

```python
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


file_path = "/content/part-00000-e9e41cbd-2ac3-4202-865b-04c1c3d0d364-c000.csv"
data = pd.read_csv(file_path)

industry_totals = data.groupby("industry")["startup_count"].sum().reset_index()

top_industries = industry_totals.sort_values(by="startup_count", ascending=False).head(10)

filtered_data = data[data["industry"].isin(top_industries["industry"])]

plt.figure(figsize=(14, 8))
sns.lineplot(data=filtered_data, x="year", y="startup_count", hue="industry", marker="o")
plt.title("Tendencias de startups por sector (2015-2024)")
plt.xlabel("Año")
plt.ylabel("Número de Startups")
plt.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Sub-Sectores")
plt.show()
plt.tight_layout()
plt.show()
```

### Visualización de los resultados
Una de las cosas que observamos fue la tendencia a la baja de la creacion de nuevos startups, que coincide con la ralentizacion de los mercados y la consolidacion de las grandes empresas.

![Alt_text](../img/tendencias_su_sector.png)
