pipeline {

    agent any

    options {
        skipDefaultCheckout(true)
        timestamps()
    }

    stages {

        stage('Checkout') {
            steps {
                echo '1. Descargando código desde GitHub...'
                checkout scm
            }
        }

        stage('Verificar entorno') {
            steps {
                echo '2. Verificando herramientas...'

                sh '''
                    git --version
                    docker --version
                    docker compose version
                '''
            }
        }

        stage('Validar Compose') {
            steps {
                echo '3. Validando docker-compose.yml...'

                sh '''
                    docker compose config --services
                '''
            }
        }

        stage('Build imágenes') {
            steps {
                echo '4. Construyendo imágenes del proyecto...'

                sh '''
                    docker compose build \
                        api \
                        dask-client \
                        spark-master
                '''
            }
        }
    }

    post {

        success {
            echo '========================================'
            echo 'PIPELINE BUILD EXITOSO'
            echo 'GitHub + Docker + Compose + Build OK'
            echo '========================================'
        }

        failure {
            echo '========================================'
            echo 'PIPELINE FALLÓ'
            echo 'Revisar etapa y salida de consola'
            echo '========================================'
        }
    }
}