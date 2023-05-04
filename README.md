# Consultando tabelas com Python e Airflow

Este projeto tem por finalidade calcular a quantidade de produtos vendidos para a cidade do Rio de Janeiro com base em dados do banco Nothwind. É um desafio proposto no ciclo básico do Programa Lighthouse, no módulo Fundamentos de Engenharia de Dados.

De forma mais detalhada, ele lê os dados da tabela "Order" inserta no banco de dados PostgreSQL (disponível em data/northwind.sql), em contâiner Docker e o arquivo "output_orders.csv". Em seguida, junta ambas as tabelas e calcula os produtos vendidos, exportando o resultado para o arquivo "count.txt".

Essa operação é orquestrada pelo Airflow, por meio de DAG construída para gerenciar o processamento da tarefa descrita.

## Como instalar
Nessa etapa, vamos descrever as instalações e os comandos a serem dados na linha de comando Linux. Antes, vale clonar este repositório com o comando:
```
git clone https://github.com/cintiadantas20/programaLighthouse_eng_dados
```
- Ambiente virtual
Primeiro é necessário criar um ambiente virtual com este comando:
```
python -m venv venv
```
Depois, ative o venv:
```
source venv/bin/activate
```
- Docker
Foi usada a ferramenta de contêiner Docker, que tambem precisa ser instalada, caso você não tenha em sua máquina. Sugiro seguir o tutorial do site https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04-pt

Após instalar, precisamos dar início ao docker:
```
sudo service docker start
```
E criar um contêiner para rodar a aplicação:
```
docker run --name desafio-airflow -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
```
Esse contêiner deve ser iniciado:
```
docker container start desafio-airflow
```
Por fim, criamos o banco de dados com o script que se encontra na pasta “data” desse repositório e o iniciamos:
```
cat northwind.sql | psql -U postgres -h localhost -p 5432 -d northwind
```
- Airflow
Nesse caso, a instalação é feita ao executar o arquivo de instalação com as configurações necessárias ao projeto, com o comando:
```
bash install.sh
```
Em seguida, limpe os DAGs de exemplo no arquivo airflow.cfg, alterando o valor da variável load_examples para False

Configure o ambiente com o comando:
```
export AIRFLOW_HOME=./airflow-data
```
Finalmente, vamos resetar o banco de dados do airflow e iniciar o airflow local:
```
airflow db reset
airflow standalone
```

## Como rodar

Abra o seu navegador na porta 8080 no link: http://localhost:8080/

Rode o airflow, inserindo o usuário e a senha fornecidos na inicialização do programa, a exemplo:

![](/imagens/imagem1.png)
 
Crie uma variável no menu Admin/Variables com seu e-mail e salve:

![](/imagens/imagem2.png)

Ligue o processamento da DAG no botão:

![](/imagens/imagem3.png)

Engatilhe a DAG:

![](/imagens/imagem4.png)

Verifique o processamento da DAG:

![](/imagens/imagem5.png)

Não se esqueça de, no final de tudo:
- Desligar a DAG no navegador e na linha de comando com um CTRL+C)
- Sair do docker com o comando sudo service docker stop
- Desativar o venv, acessando a mesma pasta da ativação e digitando “deactivate”

## Autora

- [@cintiadantas20](https://github.com/cintiadantas20)
- [@Cíntia Dantas](https://www.linkedin.com/in/cintia-dantas/)
