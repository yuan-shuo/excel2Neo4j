from neo4j import GraphDatabase
import pandas as pd

class BuildGDB:
    def __init__(self, uri: str, user: str, pwd: str):
        # 连接到Neo4j数据库  
        self.uri = uri
        self.user = user
        self.password = pwd
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def int_to_roman(num):  
        """Convert an integer to a roman numeral."""  
        val = [  
            (1000, 'M'), (900, 'CM'), (500, 'D'), (400, 'CD'),  
            (100, 'C'), (90, 'XC'), (50, 'L'), (40, 'XL'),  
            (10, 'X'), (9, 'IX'), (5, 'V'), (4, 'IV'), (1, 'I')  
        ]  
        roman_numeral = ''  
        for value, numeral in val:  
            while num >= value:  
                roman_numeral += numeral  
                num -= value  
        return roman_numeral  
    
    # 修改后的 createNode 函数  
    def createNode(self, df: pd.DataFrame):  
        with self.driver.session() as session:  
            for index, row in df.iterrows():  
                # 转换 rockClassification 为罗马数字  
                node_label = self.int_to_roman(int(row['rockClassification']))  
                print(node_label)
                
                # 跳过非1-6的 rockClassification 值（如果需要的话）  
                if not node_label or not node_label.isalpha():  # 假设非1-6的值转换结果可能不是纯字母  
                    continue  
                
                # 构建节点创建的Cypher查询  
                labels = f":{node_label}"  # 注意：Neo4j 的标签前需要加冒号  
                properties = ", ".join([f"{k}: ${k}" for k in df.columns if k != 'id'])  
                query = f"CREATE ({labels} {{{properties}}}) RETURN id(last()) as node_id"  
                
                # 执行查询并获取结果  
                parameters = {k: v for k, v in row.items() if k != 'id'}  
                result = session.run(query, **parameters)  
                # 可以在这里处理result，比如打印节点ID  
                node_id = result.single()[0]  
                print(f"Created node with ID: {node_id} and label: {node_label}")

    def read_excel_and_create_nodes(self, file_path: str, excelRow=None):
        # 读取Excel文件
        if excelRow is None:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, usecols=excelRow)  # 使用usecols参数指定列

        # 调用创建节点的方法
        self.createNode(df)

# 使用示例
if __name__ == "__main__":
    uri = "bolt://localhost:7687"  # 替换为你的Neo4j URI
    user = "neo4j"  # 替换为你的Neo4j用户名
    pwd = "1742359208ys"  # 替换为你的Neo4j密码
    gdb = BuildGDB(uri, user, pwd)
    file_path = "neoTest1.xlsx"  # 替换为你的Excel文件路径
    gdb.read_excel_and_create_nodes(file_path)