from neo4j import GraphDatabase
import pandas as pd

class BuildGDB:
    def __init__(self, uri: str, user: str, pwd: str):
        self.uri = uri
        self.user = user
        self.password = pwd
        self.driver = GraphDatabase.driver(f"{self.uri}/neo4j", auth=(self.user, self.password))
        self.roman_numerals = {  
            # 1: 'I',  
            2: 'II',  
            3: 'III',  
            4: 'IV',  
            5: 'V',  
            # 6: 'VI'  
        } 

    def main(self, excelPath, excelRow=None, deleteAll=True):
        # 删库
        if deleteAll:
            self.delBase()

        # 创建分级节点
        self.create_node_from_roman_numerals()

        # 创建特征节点
        if excelRow:
            self.read_excel_and_create_nodes(file_path=excelPath, excelRow=excelRow)
        else:
            self.read_excel_and_create_nodes(file_path=excelPath)

        self.connect_nodes_by_normal_numeral()

        # 关闭链接
        self.close()

    def delBase(self):
        query = "match(a) detach delete a"
        with self.driver.session() as session: 
            result = session.run(query)  

    def create_node_from_roman_numerals(self):  
        with self.driver.session() as session:  
            for number, roman_numeral in self.roman_numerals.items():  
                # 使用 'rockClassification' 作为标签，并添加 normalNumeral 和 romanNumeral 属性  
                query = """  
                CREATE (n:rockClassification {normalNumeral: $normalNumeral, romanNumeral: $romanNumeral})  
                RETURN n.id as node_id  
                """  
                parameters = {'normalNumeral': number, 'romanNumeral': roman_numeral}  
                result = session.run(query, parameters)  
                node_id = result.single()[0]  
  
    def close(self):  
        self.driver.close()

    def create_node_from_dataframe(self, df: pd.DataFrame):
        with self.driver.session() as session:
            for index, row in df.iterrows():
                # 构建节点创建的Cypher查询
                node_label = self.roman_numerals[int(row['rockClassification'])]
                # print(f"label is {node_label}")
                labels = f":{node_label}"
                properties = ", ".join([f"{k}: ${k}" for k in df.columns])
                query = f"CREATE ({node_label} {labels} {{{properties}}}) RETURN {node_label}.id"
                
                # 执行查询并获取结果
                result = session.run(query, **row)
                # print(f"Created node with id: {result.single()[0]}")

    def read_excel_and_create_nodes(self, file_path: str, excelRow=None):
        # 读取Excel文件
        if excelRow is None:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, usecols=excelRow)  # 使用usecols参数指定列

        # 调用创建节点的方法
        self.create_node_from_dataframe(df)

    def connect_nodes_by_normal_numeral(self):
        with self.driver.session() as session:
            # 首先，为每个normalNumeral值创建一个查询
            for normal_numeral in self.roman_numerals.keys():
                query = f"""
                MATCH (a {{normalNumeral: {normal_numeral}}})
                MATCH (b: {self.roman_numerals[normal_numeral]})
                WHERE NOT (a)-[:CONNECTED]-(b) AND a.id <> b.id
                WITH a, collect(b) as bs
                UNWIND bs AS b
                CREATE (a)-[:CONNECTED]->(b)
                """
                session.run(query)

        print("Nodes have been connected based on normalNumeral values.")

# 使用示例
if __name__ == "__main__":
    uri = "bolt://localhost:7687"  # 替换为你的Neo4j URI
    user = "neo4j"  # 替换为你的Neo4j用户名
    pwd = "1742359208ys"  # 替换为你的Neo4j密码
    gdb = BuildGDB(uri, user, pwd)
    excelRow1 = list(range(2,15))
    file_path = "neoTest1.xlsx"  # 替换为你的Excel文件路径

    gdb.main(file_path, excelRow=excelRow1)