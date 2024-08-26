# import matplotlib.pyplot as plt  
# import pandas as pd  
# import numpy as np  
# from ast import literal_eval  
  
# # 读取 CSV 文件  
# df = pd.read_csv('export.csv')  
  
# # 将字符串转换为列表，再转换为 NumPy 数组  
# df['embedding'] = df['embedding'].apply(lambda x: np.array(literal_eval(x)))  
  
# # 定义颜色映射  
# categories = df['nodeName'].unique()  
# color_map = plt.cm.get_cmap('hsv', len(categories))  
  
# # 绘制散点图  
# plt.figure(figsize=(10, 8))  
  
# for i, category in enumerate(categories):  
#     # 根据类别筛选数据  
#     category_data = df[df['nodeName'] == category]  
#     # 使用列表推导式来提取 embedding 的前两个维度  
#     x = [emb[0] for emb in category_data['embedding']]  
#     y = [emb[1] for emb in category_data['embedding']]  
#     # 绘制散点图  
#     plt.scatter(x, y, c=color_map(i), label=f'Category {category}')  
  
# # 添加图例  
# plt.legend()  
  
# # 添加标题和坐标轴标签  
# plt.title('Scatter Plot of Embedding Vectors')  
# plt.xlabel('Embedding Dimension 1')  
# plt.ylabel('Embedding Dimension 2')  
  
# # 显示图表  
# plt.show()

import matplotlib.pyplot as plt    
import pandas as pd    
import numpy as np    
from ast import literal_eval    
    
# 读取 CSV 文件    
df = pd.read_csv('export.csv')    
    
# 将字符串转换为列表，再转换为 NumPy 数组    
df['embedding'] = df['embedding'].apply(lambda x: np.array(literal_eval(x)))    
    
# 定义颜色映射    
categories = df['nodeName'].unique()    
# 尝试使用具有更高对比度的colormap，如'viridis'  
color_map = plt.cm.get_cmap('viridis', len(categories))    
    
# 绘制散点图    
plt.figure(figsize=(10, 8))    
    
for i, category in enumerate(categories):    
    # 根据类别筛选数据    
    category_data = df[df['nodeName'] == category]    
    # 使用列表推导式来提取 embedding 的前两个维度    
    x = [emb[0] for emb in category_data['embedding']]    
    y = [emb[1] for emb in category_data['embedding']]    
    # 绘制散点图    
    plt.scatter(x, y, c=color_map(i), label=f'Category {category}')    
    
# 添加图例    
plt.legend()    
    
# 添加标题和坐标轴标签    
plt.title('Scatter Plot of Embedding Vectors')    
plt.xlabel('Embedding Dimension 1')    
plt.ylabel('Embedding Dimension 2')    
    
# 保存图表    
# plt.savefig('embedding_scatter_plot.png')    
    
# 显示图表    
plt.show()