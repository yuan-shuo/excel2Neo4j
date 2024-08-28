from neo4j import GraphDatabase
import pandas as pd

class BuildGDB:
    def __init__(self, uri: str, user: str, pwd: str, dbName: str):
        self.uri = uri
        self.user = user
        self.password = pwd
        self.driver = GraphDatabase.driver(f"{self.uri}/{dbName}", auth=(self.user, self.password))
        self.roman_numerals = {  
            # 1: 'I',  
            2: 'II',  
            3: 'III',  
            4: 'IV',  
            5: 'V',  
            # 6: 'VI'  
        } 
        self.holeIDdic = {}
        self.featureNeedRow = ["id", "depth", "velocity", "strikePressure", "propelPressure",
                               "rotationPressure", "rotationVelocity", "waterPressure", "waterFlow", "time"]

    def main(self, excelPath, classification_value, excelRow=None, deleteAll=True):
        # 删库
        if deleteAll:
            self.delBase()

        # 创建分级节点
        # self.create_node_from_roman_numerals()

        # 创建特征节点
        if excelRow:
            self.read_excel_and_create_nodes(file_path=excelPath, classification_value=classification_value, excelRow=excelRow)
        else:
            self.read_excel_and_create_nodes(file_path=excelPath, classification_value=classification_value)

        # 链接分级节点和所有节点
        # self.connect_nodes_by_normal_numeral()

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

    def create_node_from_dataframe(self, df: pd.DataFrame, classification_value: int):
        # 转换DataFrame到字典
        if self.transform_df_to_dict(df):
            # 遍历字典并创建节点和关系
            with self.driver.session() as session:
                for hole_id, features_list in self.holeIDdic.items():
                    # 构建Cypher查询语句来创建节点
                    session.run(
                        "CREATE (:Hole {hole_id: $hole_id_value})",
                        {"hole_id_value": int(hole_id)}
                    )

                    for dicf in features_list:
                        # create_node_query = (
                        #     f"CREATE (:{self.roman_numerals[classification_value]} {{properties}})"
                        # )
                        # # 执行Cypher语句
                        # session.run(create_node_query, properties=dicf)
                        create_node_query = "CREATE (n1:{} {{".format(self.roman_numerals[classification_value])
                        # 将字典的键值对添加到Cypher语句中
                        for key, value in dicf.items():
                            create_node_query += f"{key}: ${key}, "
                        # 移除最后一个逗号和空格
                        create_node_query = create_node_query.rstrip(', ')

                        # create_node_query += "})"
                        create_node_query += "}) RETURN n1.id AS NodeID"
                        # 执行Cypher语句
                        res1 = session.run(create_node_query, dicf).data()[0]["NodeID"]

                        # print(f"res1={res1}")

                        # 连接Hole节点和Feature节点
                        session.run(
                            "MATCH (h:Hole), (f) "
                            "WHERE h.hole_id = $hole_node_id AND f.id = $feature_node_id "
                            "CREATE (h)-[:HAS_FEATURE]->(f)",
                            {"hole_node_id": hole_id, "feature_node_id": res1}
                        )

    def read_excel_and_create_nodes(self, file_path: str, classification_value: int, excelRow=None):
        # 读取Excel文件
        if excelRow is None:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, usecols=excelRow)  # 使用usecols参数指定列

        # 筛选'rockClassification'列值为指定分类值的行
        df_filtered = df.query('rockClassification == @classification_value')

        # 调用创建节点的方法
        self.create_node_from_dataframe(df_filtered, classification_value)

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

    def transform_df_to_dict(self, df):
        # 确保holeID列存在，否则抛出错误
        if 'holeID' not in df.columns:
            raise ValueError("Input DataFrame must contain 'holeID' column")

        # 遍历DataFrame中的每一行
        for index, row in df.iterrows():
            # 获取当前行的holeID
            hole_id = row['holeID']
            
            # 如果holeID不在字典中，初始化一个空列表
            if hole_id not in self.holeIDdic:
                self.holeIDdic[hole_id] = []
            
            # 基于self.featureNeedRow列的值创建一个字典
            feature_dict = {column: row[column] for column in self.featureNeedRow}
            
            # 将创建的字典添加到对应holeID的列表中
            self.holeIDdic[hole_id].append(feature_dict)
        
        return True

# 使用示例
if __name__ == "__main__":
    uri = "bolt://localhost:7687"  # 替换为你的Neo4j URI
    user = "neo4j"  # 替换为你的Neo4j用户名
    pwd = "1742359208ys"  # 替换为你的Neo4j密码
    gdb = BuildGDB(uri, user, pwd, dbName="neo4j")
    excelRow1 = list(range(0,15))
    file_path = "neoTest1.xlsx"  # 替换为你的Excel文件路径

    gdb.main(file_path, classification_value=3, excelRow=excelRow1)