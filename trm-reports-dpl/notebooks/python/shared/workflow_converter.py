# Databricks notebook source
# MAGIC %sh
# MAGIC pip install matplotlib
# MAGIC pip install networkx

# COMMAND ----------


class NodeElement(object):

    def __init__(self, node,root):
        self.tool_id = node.attrib['ToolID']
        self.plugin = node.find('GuiSettings').attrib.get('Plugin')
        self.x_pos = float(node.find('GuiSettings').find('Position').attrib['x'])
        self.y_pos = float(node.find('GuiSettings').find('Position').attrib['y'])
        self.tool = self.plugin.split('.')[-1] if self.plugin else None
        self.description = None
        self.ljoin_fields = []
        self.rjoin_fields = []
        self.select_fields = []
        
        self.query = ""
        self.sparkquery = ""
        if self.plugin == 'AlteryxBasePluginsGui.Join.Join':
            join_data = node \
                .find('Properties') \
                .find('Configuration') \
                .findall('JoinInfo')
            ljoin_data = join_data[0]
            rjoin_data = join_data[1]
            self.ljoin_fields = []
            lj =""
            rj=""
            for field in ljoin_data.findall('Field'):
                self.ljoin_fields.append(field.attrib['field'])
                lj = lj + "," + field.attrib['field']
            self.rjoin_fields = []
            for field in rjoin_data.findall('Field'):
                self.rjoin_fields.append(field.attrib['field'])
                rj = rj + "," + field.attrib['field']
            join_type = "inner"
            joinstr =""
            for i in range(0,len(self.ljoin_fields)) :
                joinstr=joinstr+"ldfs(\""+self.ljoin_fields[i] + "\")===rdfs(\""+self.rjoin_fields[i]+ "\"),"
                

            self.query = "select * from " + lj + rj
            self.sparkquery = "val df" + self.tool_id + " = ldfs.join(rdfs," + joinstr[:-1] + f",{join_type})"

        

        elif self.plugin == 'AlteryxGuiToolkit.ToolContainer.ToolContainer':
            self.description = node \
                .find('Properties') \
                .find('Configuration') \
                .find('Caption').text
            
        
        elif self.plugin == 'AlteryxSpatialPluginsGui.Summarize.Summarize':
            self.summarize_fields = node \
                .find('Properties') \
                .find('Configuration') \
                .find('SummarizeFields')
            self.summarize_fields = [field.attrib for field in self.summarize_fields]
            group_by = []
            aggregations = []
            for field in self.summarize_fields:
                if field['action'] == 'GroupBy':
                    group_by.append("\""+ field['field'] + "\"")
                elif field['action'] == 'Sum':
                    aggregations.append(f'sum(\"{field["field"]}\").alias(\"{field["rename"]}\")')
            group_by_clause = ', '.join(group_by)
            aggregation_clause = ', '.join(aggregations)
            self.sparkquery = f"val df{self.tool_id} = df.groupBy({', '.join(group_by)}).agg({', '.join(aggregations)})"



        elif self.plugin == 'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect':
            self.select_fields = node \
                .find('Properties') \
                .find('Configuration') \
                .find('SelectFields') \
                .findall('SelectField')
            self.select_fields = [field.attrib for field in self.select_fields]
            for e in self.select_fields:
                if(e['selected']=='True'):
                    rename = ""
                    if("rename" in e):
                        rename = e['rename']
                        self.query =self.query+ e['field']+ " as "+rename +","
                        self.sparkquery = self.sparkquery + "\"" + e['field']+ " as "+rename+ "\"" +","
                    else:
                        self.query =self.query+ e['field'] +","
                        self.sparkquery = self.sparkquery + "\"" + e['field']+ "\""  +","

            self.sparkquery = "val df"+self.tool_id+" = df.selectExpr(" + self.sparkquery[:-1] + ")"


        


        elif self.plugin == 'AlteryxBasePluginsGui.Formula.Formula':
            self.sparkquery = node \
                .find('Properties') \
                .find('Configuration') \
                .find('FormulaFields') \
                .findall('FormulaField')
            self.sparkquery = [field.attrib for field in self.sparkquery]
            filt = ""
            for e in self.sparkquery:
                if 'IF' in e['expression']:
                    filt = filt + ".withColumn(\""+e['field']+"\",expr(\""+e['expression'].replace('\"', '\'').replace('ENDIF'," end ").replace('ELSEIF'," when ").replace('IF',"case when ").replace('[',' ').replace(']',' ') + "\"))"
                elif (e['type']=='V_WString'):
                    filt = filt + ".withColumn(\""+e['field']+"\",lit(\""+e['expression'].replace('[','').replace(']','') + "\"))"
                else:
                    filt = filt + ".withColumn(\"" + e['field']+"\",lit("+e['expression'].replace('[','').replace(']','')  + "))"

            self.sparkquery = "val df"+self.tool_id+" = df"+filt.replace('\n', ' ')

        # else:
        #     self.select_fields = None

        elif self.plugin == 'AlteryxBasePluginsGui.AppendFields.AppendFields':
            # self.sparkquery = node \
            #     .find('Properties') \
            #     .find('Configuration') \
            #     .find('FormulaFields') \
            #     .findall('FormulaField')
            # self.sparkquery = [field.attrib for field in self.sparkquery]
            filt = ""
            # for e in self.sparkquery:
            #     if(e['type']=='V_WString'):
            #         filt = filt + ".withColumn("+e['field']+",lit(\""+e['expression'] + "\"))"
            #     else:
            #         filt = filt + ".withColumn(" + e['field']+",lit("+e['expression'] + "))"

            self.sparkquery = "val df"+self.tool_id+" = df"+filt

        # else:
        #     self.select_fields = None

        elif self.plugin == 'AlteryxBasePluginsGui.Unique.Unique':
            self.unique_fields = node \
                .find('Properties') \
                .find('Configuration') \
                .find('UniqueFields') \
                .findall('Field')
            self.unique_fields = [field.attrib for field in self.unique_fields]
            for e in self.unique_fields:
                self.sparkquery = self.sparkquery + "\"" + e['field']+ "\""  +","

            self.sparkquery = "val df"+self.tool_id+" = df.dropDuplicates(" + self.sparkquery[:-1] + ")"

        # else:
        #     self.select_fields = None
        elif self.plugin == 'AlteryxBasePluginsGui.Union.Union':

            self.sparkquery = "val df"+self.tool_id+" = ldfs.UnionByName(rdfs)"


        elif self.plugin == 'AlteryxBasePluginsGui.Sort.Sort':
            self.sort_fields = node \
                .find('Properties') \
                .find('Configuration') \
                .find('SortInfo') \
                .findall('Field')
            self.sort_fields = [field.attrib for field in self.sort_fields]
            for e in self.sort_fields:
                if(e['order']=='Ascending'):
                    self.sparkquery = self.sparkquery + "\"" + e['field']+ "\""  +","
                else:
                    self.sparkquery = self.sparkquery + "desc(\"" + e['field']+ "\")"  +","


            self.sparkquery = "val df"+self.tool_id+" = df.orderBy(" + self.sparkquery[:-1] + ")"


        elif self.plugin == 'AlteryxBasePluginsGui.Filter.Filter':
            self.filter_fields = node \
                .find('Properties') \
                .find('Annotation') \
                .find('DefaultAnnotationText').text
            
            cond = self.filter_fields.replace('[','').replace(']','').replace(' = ',' == ').replace('"','\'')
            self.sparkquery = "val df"+self.tool_id+" = df.where(\"" +cond+  "\")"

        # else:
        #     self.select_fields = None



        elif self.plugin == 'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput':
            self.sparkquery = "df.write.format().save()"
        
        elif self.plugin == 'AlteryxBasePluginsGui.DbFileInput.DbFileInput':
            self.filter_fields = node \
                .find('Properties') \
                .find('Configuration') \
                .find('File')
            if self.filter_fields is not None:
                file_content = self.filter_fields.text
                query = file_content.split('|||')[1] if '|||' in file_content else ''
                self.sparkquery = f'val df{self.tool_id} = spark.read.format("jdbc").option("url", "jdbc:odbc:DSN=PROJECT_SQL").option("dbtable", "{query}").load()'
            else:
                self.sparkquery = f'val df{self.tool_id} = spark.read.format("jdbc").load()'


        elif self.plugin == 'AlteryxBasePluginsGui.TextInput.TextInput':
            self.sparkquery = f'val df{self.tool_id} = spark.read.format("text").load()'


        
        elif self.plugin == 'AlteryxBasePluginsGui.BrowseV2.BrowseV2':
            self.sparkquery = "val df"+self.tool_id +".show()"



        elif  (self.plugin == 'AlteryxGuiToolkit.ToolContainer.ToolContainer' and node.find('Properties').find('Configuration').find('Caption').text == 'Delta tool - DO NOT MODIFY'):
            self.delta =  node \
                .find('Properties') \
                .find('Configuration') \
                .findall('Value')
            c=0
            self.deltaquery=""
            for field in self.delta:
                print(field.text)
                if c==0:
                    for m in field.text.split(","):
                        if(m.endswith("True") and c==0):
                            print(m.split("=")[0])
                            self.deltaquery = self.deltaquery+"\""+m.split("=")[0] +"\","
                    self.sparkquery = "val df"+self.tool_id +"_new=ldfs.as(\"a\").join(rdfs.as(\"b\"), Seq("  +  self.deltaquery[:-1] +"),\"left anti\")\n"
                    self.sparkquery =  self.sparkquery  + "val df"+self.tool_id +"_delete=rdfs.as(\"a\").join(ldfs.as(\"b\"), Seq("  +  self.deltaquery[:-1] +"),\"left anti\")\n"
                    self.sparkquery = self.sparkquery  + "\n val df"+self.tool_id +"_change=ldfs.as(\"a\").join(rdfs.as(\"b\"), Seq("  +  self.deltaquery[:-1] +"),\"inner\")"
                    c=1
                else:
                    self.deltaquery=""
                    for m in field.text.split(","):
                        if(m.endswith("True") and c==1):
                            self.deltaquery =  self.deltaquery+"coalesce(col(\"a."+m.split("=")[0] +"\"), lit(\"\"))=!= coalesce(col(\"b."+m.split("=")[0] +"\"), lit(\"\")) or "
                    self.sparkquery = self.sparkquery +".where("+ self.deltaquery[:-3]+")"

        
        else:
            self.ljoin_fields = None
            self.rjoin_fields = None
            self.select_fields = None
            try:
                self.description = node \
                    .find('Properties') \
                    .find('Annotation') \
                    .find('DefaultAnnotationText').text
            except:
                self.description = None


        # if(self.sparkquery == ""):
        #        self.sparkquery= "val df"+self.tool_id + "=df."
        # else:
        #         self.sparkquery
        


        self.description = self.description.replace('\n', ' ') if self.description else None
        
        
        for connection in root.find('Connections').iter('Connection'):
             dest = connection.find('Destination').attrib.get('ToolID')
             if(dest==self.tool_id):
                 origin = connection.find('Origin').attrib.get('ToolID')
                 if(origin and connection.find('Destination').attrib.get('Connection')=='Left'):
                     print("1origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('ldfs', 'df'+origin) if self.sparkquery else None
                 if(origin and connection.find('Destination').attrib.get('Connection')=='Right'):
                     print("2origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('rdfs', 'df'+origin) if self.sparkquery else None
                 if(origin and connection.find('Destination').attrib.get('Connection')=='New Records'):
                     print("3origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('ldfs', 'df'+origin+'_new') if self.sparkquery else None
                 if(origin and connection.find('Destination').attrib.get('Connection')=='Changed Records'):
                     print("4origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('rdfs', 'df'+origin+'_change') if self.sparkquery else None
                 if(origin and connection.find('Destination').attrib.get('Connection')=='New Data Stream'):
                     print("5origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('ldfs', 'df'+origin) if self.sparkquery else None
                 if(origin and connection.find('Destination').attrib.get('Connection')=='Old Data Stream'):
                     print("6origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('rdfs', 'df'+origin) if self.sparkquery else None
                 if(origin and connection.find('Origin').attrib.get('Connection')=='New Records'):
                     print("7origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('df.', 'df'+origin+'_new.') if self.sparkquery else None
                 if(origin and connection.find('Origin').attrib.get('Connection')=='Changed Records'):
                     print("8origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('df.', 'df'+origin+'_change.') if self.sparkquery else None
                 if(origin and connection.attrib.get('name')=='#1'):
                     print("1origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('ldfs', 'df'+origin) if self.sparkquery else None
                 if(origin and connection.attrib.get('name')=='#2'):
                     print("2origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('rdfs', 'df'+origin) if self.sparkquery else None
                 if(origin):
                     if(self.sparkquery == ""):
                        self.sparkquery= "val df"+self.tool_id + "=df."
                     else:
                        self.sparkquery
                     print("9origin="+origin+"dest = "+dest) 
                     self.sparkquery = self.sparkquery.replace('df.', 'df'+origin+'.') if self.sparkquery else None
                 





        self.data = {
            'Tool ID': self.tool_id,
            'Plugin': self.plugin,
            'Tool': self.tool,
            'Description': self.description,
            'x': self.x_pos,
            'y': self.y_pos,
            'Left Join Fields': self.ljoin_fields,
            'Right Join Fields': self.rjoin_fields,
            'Select Fields': self.select_fields,
            # 'Query':  self.query.replace("\"\"","\"").replace("\"v","v"),
            'Spark Query': self.sparkquery
        }


# COMMAND ----------

import xml.etree.ElementTree as ET
import csv
import matplotlib.pyplot as plt
from shutil import copyfile
import sys
import networkx as nx

#provide below three names

file = "./tm_filings.xml"    # yxmd filename
output_file_name = "./TM_Filings_Dashboard_converted.csv"             # output file name
dag_name = "./dagname.png"                 # Dag Nam


assert len(file.split('.')) > 1, 'Input file must have an extension'
file_ext = file.split('.')[-1]
assert file_ext == 'xml' or file_ext == 'yxmd', 'Input file must be .xml or .yxmd'
if file_ext == 'yxmd':
    xml = file.split('.')[0] + '.xml'
    # copyfile(file, xml)
    tree = ET.parse(xml)
else:
    tree = ET.parse(file)      
assert len(output_file_name.split('.')) > 1, 'Output file must have an extension'
output_file_ext = output_file_name.split('.')[-1]
assert output_file_ext == 'csv', 'Output file must be .csv'
graph = nx.DiGraph()
root = tree.getroot()
print(root)
lst = []
for x in root.iter('Node'):
    node = NodeElement(x,root)
    lst.append(node.data)
    graph.add_node(node.data['Tool ID'])
    

for connection in root.find('Connections').iter('Connection'):
    
    connected_tool_id = connection.find('Origin').attrib.get('ToolID')
    graph.add_edge(connected_tool_id, connection.find('Destination').attrib.get('ToolID'))


mst=[]
for node in nx.algorithms.topological_sort(graph):
    # print("massa"+node)
    # print(int(node))
    mst = mst + ([d for d in lst if int(d.get('Tool ID')) == int(node)])

G = nx.DiGraph()

for connection in root.find('Connections').findall('Connection'):
    origin_tool_id = connection.find('Origin').attrib['ToolID']
    destination_tool_id = connection.find('Destination').attrib['ToolID']
    G.add_edge(origin_tool_id, destination_tool_id)


pos = nx.spring_layout(nx.algorithms.topological_sort(graph), seed=142)
for node_data in lst:
    tool_id = node_data['Tool ID']
    x = node_data.get('x')  
    y = node_data.get('y')
    if x is not None and y is not None:
        pos[tool_id] = (int(x), int(y))
plt.figure(figsize=(60, 20))
nx.draw(G, pos, with_labels=True, node_size=1000, node_color='skyblue', font_size=10, font_color='black', font_weight='bold', arrowsize=20)

edge_labels = {(u, v): v for u, v in G.edges}
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')

plt.title("Directed Acyclic Graph (DAG)")

plt.savefig(dag_name, format='png', bbox_inches='tight')

with open(output_file_name, 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, mst[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(mst)



# COMMAND ----------


