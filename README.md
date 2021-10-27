# Desafio - Pergunta
  Diante do cenário proposto o ideal seria realizar os cálculos de forma paralela e distribuída, utilizando **Apache Spark**, por exemplo. Desta forma, cada **cluster** é responsável por calcular parte das transações, permitindo o escalonamento da arquitetura conforme a volumetria dos dados.  A utilização de um *Data Lake* também é algo a ser pensado, visto que os dados brutos (**raw**) serão armazenados na primeira camada e conforme a necessidade ocorre o processo de ETL para a camada seguinte (**trusted**), onde os dados são enriquecidos, evitando a sobrecarga no processamento.  

  O particionamento dos dados também é importante a fim de otimizar a sua leitura e salvar os dados em formato colunar na camada **trusted**, por exemplo no formato **Apache Parquet**, torna a sumarização dos dados mais eficiente. Ainda, nesta camada é necessário adotar algumas medidas de governança e segurança dos dados, como criptografar ou mascarar dados sensíveis, tendo em vista que se trata de transações financeiras. 

  Por fim, as 200 milhões de transações poderiam ser sumarizadas/calculadas diariamente ou através de um *treshhold*  ( tamanho dos arquivos, tempo, quantidade de arquivos). No fim do mês bastaria ler os dados sumarizados/calculados neste período e contabilizar o valor total, evitando realizar o cálculo de aproximadamente 6 bilhões de transações mensais de uma única vez.  

 
