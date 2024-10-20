FROM quay.io/astronomer/astro-runtime:12.1.0

# Troque para o usuário root para instalar pacotes adicionais
USER root

# Adicione a assinatura para confiar no repositório da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc

# Adicione o repositório à lista de fontes do apt
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

# Instale o driver ODBC do SQL Server
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    apt-get install -y unixodbc-dev

# Carregue as variáveis de ambiente
RUN source ~/.bashrc

# # Instalação do driver ODBC da Microsoft para SQL Server
# RUN apt-get update && \
#     apt-get install -y curl apt-transport-https gnupg2 unixodbc-dev && \
#     curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list -o /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && \
#     apt-get install -y openssl && \
#     ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev && \
#     echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc && \
#     echo 'export ODBCINI=/etc/odbc.ini' >> ~/.bashrc && \
#     echo 'export ODBCSYSINI=/etc' >> ~/.bashrc && \
#     echo 'export LD_LIBRARY_PATH=/opt/microsoft/msodbcsql17/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc 

# Troque de volta para o usuário airflow
USER astro



