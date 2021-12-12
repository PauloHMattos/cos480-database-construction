# Avaliação das Organizações de Arquivos de Dados  (métodos de acesso primário)

Desenvolver uma banca de teste para a comparação do desempenho de um conjunto de Métodos de Acesso entre as três organizações primárias de arquivos de blocos de registros” (file of blocks of records), comumente usadas em SGBD Relacionais. Os indicadores de desempenho a serem levantados serão: 

1. **Espaço utilizado:** a ser computado através do total de blocos de memória  secundária utilizados pelo arquivo de registros e pelas eventuais estruturas de apoio.
2. **Número de Acessos:** a ser computados através da quantidade de blocos acessados pelo método de acesso conforme especificado abaixo. Essa medidas dará uma indicação indireta a respeito do espaço e do tempos de acesso necessários à execução das operações com a organização primária adotada.



## Os Métodos de Acesso a serem programados são os seguintes:

1. **(INSERT):** 
    1. Inserção de um único registro.
    2. Inserção de um conjunto de registros.

2. **(SELECT):**
    1. Seleção de um único registro (Find) pela sua _chave primária_ (ou campo UNIQUE). Por exemplo, selecionar o registro de um ALUNO pelo seu DRE fornecido.
    2. Seleção de conjunto de registros (FindAll) cujas _chave primária_ (ou campo UNIQUE) estejam contidas em um conjunto de valores não necessariamente sequenciais. Por exemplo, selecionar todos os registros dos ALUNOS cuja lista de DRE estão inscritos na TURMA de código 1020.
    3. Seleção de um conjunto de registros (FindAll) cujas _chave primária_ (ou campo UNIQUE) estejam contidas em uma faixa de valores dado como parâmetro, por exemplo, todos os ALUNOS cujo DRE esteja na faixa entre "119nnnnnn" e "120nnnnnn" (onde "n" é qualquer dígito de 0 a 9, ou em SQL: ... **where** DRE **between** 119000000 and 120999999
    4. Seleção de todos os registros (FindAll) cujos valores de um campo _não chave_ sejam iguais a um dado parâmetro fornecido. Ou seja, seleção por um campo que permite repetição de valores entre registros. Por exemplo, recuperar todos os registros das PESSOAS cujo campo CIDADE seja igual a "Rio de Janeiro".

3. **(DELETE):**
    1. Remoção de um único registro selecionado através da chave primária (ou campo cujo valor seja único), por exemplo, remover o ALUNO cujo DRE é dado.
    2. Remoção de um conjunto de registros selecionados por algum critério, por exemplo, remover todos os ALUNOS da tabela INSCRITOS cuja turma seja a de NUMERO=1023.

4. **(Reorganizar):**
    1. Reorganizar o arquivo retirando os registros marcados como _removidos_, diminuindo o tamanho do arquivo original. Essa operação será executado periodicamente segundo certos critérios.
    2. Reorganizar arquivo com organização primária _sequencial ordenado_, que possuem arquivos auxiliares de Extensão (conforme especificado a seguir), utilizando algoritmos como o _Merge Sort Externo_.

A operação de UPDATE foi considerada como sendo uma remoção seguida de uma inserção e, portanto, **não** deverá ser programada.




## Organizações Primárias a serem testadas
Cada um desses métodos deverá ser desenvolvido para as seguintes organizações primárias do arquivo de registros:

1. **Heap**, ou _arquivo sem qualquer ordenação_, com registros de _tamanho fixos_, inserção de novos ao final do arquivo, e _remoção_ baseada em lista encadeada dos registros removidos, que deverão ser reaproveitados em uma nova inserção posterior a remoção.
2. **Heap**, ou sequencial sem qualquer ordenação, com registros de tamanho variável (por exemplo, campos strings, campos multivalorados, etc.). Os campos de tamanho variável podem ser organizados por _caracteres de marcação_, ou por _Record Head_ contendo (posição, tamanho) dos cada campos de tamanho variável. As inserções deve ser ao final do arquivo, e os registros _removidos_ deverão ser apenas marcados. O espaço dos registros deletados deverá ser retirado do arquivo através de processo periódico de _Reorganização_ do mesmo, quando atingido certos critérios.
3. Arquivo **ordenado** por um _campo de ordenação_ escolhido, com registros de _tamanho fixo_. As inserções de novos registros deverão ocorrer em um arquivo de _Extensão_ com posterior reordenação, após certo critério ser atingido, entre o arquivo Principal e a sua Extensão. Remoção através de marcação do registro deletado, com posterior _Reorganização_ do arquivo, retirando os espaços dos registros removidos. Essa reorganização pode se dar em conjunto com a _reordenação_ de junção com o arquivo auxiliar de Extensão.
4. **Hash externo estático**, distribuídos segundo uma campo _chave de hashing_ (ou campo cujo valor seja único), usando como tamanho do bucket um múltiplo do tamanho dos blocos de memória, tratamento de colisão por lista em _conjunto de overflow buckets_. Pode ser utilizada uma função de hashing básica, do tipo “função módulo”, usando o número de buckets alocados, ou utilizar outra que melhor se adequa ao(s) domínio(s) do campo(s) de hashing escolhido(s) (por exemplo, pela primeira letra de um campo string que distribui entre 27 buckets sequenciais por ordem alfabética do resultado do hashing). Se for usar uma função de hashing que mantém a ordenação, como no exemplo anterior, isso deverá ser assinalado no trabalho.




## Orientações Gerais
Os registros podem todos serem de tamanho menor que o tamanho do bloco escolhido (ou seja, cada bloco de memória pode acomodar um conjunto de registros). No caso das organizações por _ordenação_ e _hash_, os registros pode todos serem de tamanho fixo, com atributos (campos) igualmente de tamanho fixo. A _blocagem_ dos registros pode ser _unspanned_ (ou seja, sem quebra de registros entre blocos). Todavia, o grupo que quiser usar registros de tamanho variável, ou mesmo _blocagem spanned_, ou mesmo testar registros maiores que um bloco, está livre para apresentar essas variações devidamente assinaladas no trabalho entregue.


Todo Arquivo de Registros, em qualquer das organização testadas, deverá ter um FILE HEAD contendo informações gerais, tais como, indicação do esquema de dados que o arquivo codifica (em geral o nome da tabela relacional), número de registros no arquivo, fator de blocagem, tamanhos e localizações relativas dos campos fixos e variáveis dentro dos registros, ponteiro para o início da lista de registros deletados, _timestamps_ de criação e alteração do arquivo, e o que mais o grupo achar conveniente registrar sobre a estrutura do arquivo de registros.

A base de dados de entrada deverá estar em formato texto (.CSV ou outro qualquer) e ser processada para gerar as inserções em lote. A escolha da base é livre, porém deverá ter um tamanho que seja maior que a memória RAM do computador (para provocar o I/O de disco) e ordenação externa. Há diversos sites de geração de bases fakes (https://www.guiadoexcel.com.br/base-de-dados-teste-excel/) ou repositórios com bases reais, tais como, os _repositórios públicos nacionais_ e estrangeiros :

1. “Portal Brasileiro de Dados Abertos”: http://dados.gov.br/
2. IBGE: https://www.ibge.gov.br/pt/inicio.html aba Estatísticas
3. Armazém de dados do IPP/Rio de Janeiro: http://www.data.rio/
4. Dados de SP: http://www.governoaberto.sp.gov.br/
5. Datasus: http://www.datasus.gov.br
6. Free Databases: https://www.ebsco.com/products/research-databases/free-databases
7. Relativo a mortalidade, SOA, https://www.mortality.org/, 
8. Nasa, DOAJ, SSRN, PLS, OpenDOAR, BASE, DBPL Etc.... 



## O resultado a ser entregue será na forma de:
1. Um relatório descrevendo todas as opções adotadas no desenvolvimento da bancada de teste, tais como, o tamanho do bloco de memória adotado, o fator de blocagem, a descrição da base de dados usada no teste, a descrição dos campos, das chaves, das estruturações dos arquivos adotadas, campos utilizados em ordenações, funções de Hash adotada, a estrutura dos HEADs dos arquivos, etc; 
2. Link para o repositório (tipo Git) contendo a hierarquia de arquivos de programas do projeto da bancada de teste, bem como  todo os arquivos auxiliares utilizados e gerados, em particular o Projeto Funcional da bancada, incluindo o Projeto Descritivo de cada rotina programada (no mais simples, o pseudo código da mesma).
3. Tabelas resumo com os valores dos indicadores de desempenhos produzidos durante os testes.
Um texto contendo uma conclusão comparativa a respeitos dos resultados obtidos, finalizando com a recomendação de qual organização primária utilizar para cada "caso" de bases de dados e aplicação (parta de exemplos específicos)
