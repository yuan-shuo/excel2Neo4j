def sum_values_of_key(features_list, key):
    total_sum = 0
    for feature_dict in features_list:
        if key in feature_dict:  # 检查键是否存在于字典中
            total_sum += feature_dict[key]  # 累加该键对应的值
    return total_sum

# 示例使用
features_list = [
    {'depth': 10, 'velocity': 0.5},
    {'depth': 20, 'velocity': 0.3},
    {'depth': 15, 'velocity': 0.7}
]

key = 'velocity'
print(sum_values_of_key(features_list, key))  # 输出应该是 0.5 + 0.3 + 0.7 的总和