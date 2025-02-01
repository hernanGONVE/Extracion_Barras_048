import pandas as pd
import numpy as np
import os

def importador_txt(lista_registros:list):
    dict_trimestre={}
    for i in range(len(lista_registros)):
        elemento    =   ""
        elemento  =   str(lista_registros[i])
        df=pd.read_csv(elemento, sep='\t')
        dict_trimestre[str(i+1)]  =   df
    return dict_trimestre


def nombre_barras(diccionario_global: dict) -> list:
    suministros = set()
    
    for n in diccionario_global.keys():
        suministros.update(diccionario_global[n]["Código de barra"].unique())  
    
    return list(suministros) 


def separador_suministros(dict_meses:dict , suministros:list,espacio_busqueda:str):
    copia_dict_meses    =   dict_meses.copy()
    dict_suministros    =   {}
    df_sumador= pd.DataFrame()
    for k in range(len(dict_meses)):
        df_sumador = pd.concat([df_sumador, dict_meses[str(k+1)]], ignore_index=True)


    for i in suministros:

        df_suministo  =   df_sumador[df_sumador[espacio_busqueda].isin([str(i)])]
        dict_suministros[str(i)]    =   df_suministo

    return dict_suministros


def formato(df_dict:dict):
    out_dict={}
    for x in list(df_dict.keys()):
        df_dict[x]=(df_dict[x]).rename({"Registro de energía activa (kWh)":x+" E.Act(kwh)","Registro de energía reactiva (kVARh)":x+" E.Rea(kVARh)"},axis=1)
        out_dict[x]=df_dict[x][["Fecha (AAAAMMDDHHMM)",x+" E.Act(kwh)",x+" E.Rea(kVARh)"]]
    return out_dict


def formato_trafos(df_dict:dict):
    out_dict={}
    for x in list(df_dict.keys()):
        df_dict[x]=(df_dict[x]).rename({"Registro de energía activa (kWh)":x+" E.Act(kwh)","Registro de energía reactiva (kVARh)":x+" E.Rea(kVARh)"},axis=1)
        out_dict[x]=df_dict[x][["Código de transformador","Fecha (AAAAMMDDHHMM)",x+" E.Act(kwh)",x+" E.Rea(kVARh)"]]
    return out_dict


def funcion_agrupar(dic,base):
    for x in list(dic.keys()):
        base=pd.merge(dic[x],base,on="Fecha (AAAAMMDDHHMM)", how="right")
    
    return base


def funcion_agrupar_trafos(dic, base):

    if not isinstance(base.columns, pd.MultiIndex):
        base.columns = pd.MultiIndex.from_tuples([(col, '') for col in base.columns])

    keys = list(dic.keys())

    base = dic[keys[0]].copy()

    base[('Fecha (AAAAMMDDHHMM)', '')] = base[('Fecha (AAAAMMDDHHMM)', '')].astype(str)

    for x in keys[1:]:

        dic[x].columns = pd.MultiIndex.from_tuples([tuple(col) for col in dic[x].columns])

        dic[x][('Fecha (AAAAMMDDHHMM)', '')] = dic[x][('Fecha (AAAAMMDDHHMM)', '')].astype(str)

        print(f"🔹 Fusionando con {x}")
        print(f"  - Fechas en base: {base[('Fecha (AAAAMMDDHHMM)', '')].unique()[:5]} ...")
        print(f"  - Fechas en {x}: {dic[x][('Fecha (AAAAMMDDHHMM)', '')].unique()[:5]} ...")

        base = pd.merge(
            base, dic[x], 
            on=[('Fecha (AAAAMMDDHHMM)', '')],  
            how="outer"
        )

    return base



def detector_empresas(dic):
    una_empresa={}
    for x in list(dic.keys()):
        if len(list(dic[x]["Código de transformador"].unique()))    ==   1:
            una_empresa[x]=dic[x]
            dic.pop(x)
    return [una_empresa,dic]


#================================================================
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#--------------------Valores de Ingreso--------------------------

anio=2023 # Año de la data(VALOR ENTERO)
bisiesto= 0   # 0 para no y 1 para si(VALOR BINARIO)
ad_nombre = "AD01" # nombre de la carpeta en donde se encuentran todos los archivos en formato "txt."

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#================================================================

archivo_txt = [os.path.join(ad_nombre, archivo) for archivo in os.listdir(ad_nombre) if os.path.isfile(os.path.join(ad_nombre, archivo))]

dict_trimestres = importador_txt(archivo_txt)
suministros=nombre_barras(dict_trimestres)
dict_suministros=separador_suministros(dict_trimestres,suministros,"Código de barra")


alineado={}
no_alineado={}
for x in list(dict_suministros.keys()):

    if len(list(dict_suministros[x]["Fecha (AAAAMMDDHHMM)"])) == len(list(dict_suministros[x]["Fecha (AAAAMMDDHHMM)"].unique())):
        alineado[x]=dict_suministros[x]
    else:
        no_alineado[x]=dict_suministros[x]



meses={"01":31,"02":28+bisiesto,"03":31,"04":30,"05":31,"06":30,"07":31,"08":31,"09":30,"10":31,"11":30,"12":31}
horas=["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
minutos=["00","15","30","45"]
Fecha=[]
for mes in list(meses.keys()):
    for dia in range(meses[mes]):
        for hora in horas:
            for minuto in minutos:
                if dia <9:
                    fecha_basica=str(anio)+mes+"0"+str(dia+1)+hora+minuto
                else:
                    fecha_basica=str(anio)+mes+str(dia+1)+hora+minuto
                Fecha+=[fecha_basica]
Fecha=Fecha+[str(anio+1)+"01010000"]

df_base=pd.DataFrame( {"Fecha (AAAAMMDDHHMM)":Fecha})
df_base=df_base.astype("int64")


alineado=formato(alineado)
primer_grupo= funcion_agrupar(alineado,df_base)


[una_empresa    ,   mas_empresas]=detector_empresas(no_alineado)

print("\n De los valores no alineados, los que solo son alimentados por una empresa distribuidora son: ")
print(una_empresa.keys())
print("\n Los restantes tienen mas de una empresa que los alimenta: ")
print(mas_empresas.keys())


for n in list(una_empresa.keys()):
    una_empresa[n]=una_empresa[n].drop_duplicates("Fecha (AAAAMMDDHHMM)",keep="first")
una_empresa

una_empresa=formato(una_empresa)

segundo_grupo=funcion_agrupar(una_empresa,df_base)

consolido_grupos=pd.merge(segundo_grupo,primer_grupo,on="Fecha (AAAAMMDDHHMM)")

#================================================================================================
with pd.ExcelWriter("barra_"+ad_nombre+".xlsx") as writer:
    consolido_grupos.to_excel(writer,sheet_name="barra AD13")


#================================================================================================


formato_mas_empresas    =formato_trafos(mas_empresas)

formato_mas_empresas_pivot={}
for x in formato_mas_empresas.keys():
    formato_mas_empresas_pivot[x] = formato_mas_empresas[x].pivot(index ="Fecha (AAAAMMDDHHMM)", columns="Código de transformador",   values=[x+" E.Act(kwh)",x+" E.Rea(kVARh)"]).reset_index()


consolidados_trafos =   funcion_agrupar_trafos(formato_mas_empresas_pivot, df_base)

with pd.ExcelWriter(ad_nombre+"_trafos.xlsx") as writer:
    consolidados_trafos.to_excel(writer,sheet_name="barra AD13")


#================================================================================================================
agrupado=pd.DataFrame()
for mm in list(dict_trimestres.keys()):
    dict_tri = dict_trimestres[mm][['Código de subestación', 'Código de transformador', 'Número de serie del transformador', 'Código de barra']]
    dict_tri = dict_tri.drop_duplicates() 
    if not agrupado.empty:
        agrupado = pd.concat([agrupado,dict_tri ], ignore_index=True)
    else:
        agrupado = dict_tri.copy()

with pd.ExcelWriter("barra_"+ad_nombre+"_resumen.xlsx") as writer:
    agrupado.to_excel(writer,sheet_name="barras "+ad_nombre)
#================================================================================================================
print("Finalizo")
