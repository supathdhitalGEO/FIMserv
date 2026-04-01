import fimserve as fm

user_boundary = "./path/to/your/boundary file"

summary = fm.getIntersectedHUC8ID(user_boundary, area=True)
print(summary)
