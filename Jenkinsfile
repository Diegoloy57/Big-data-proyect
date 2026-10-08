pipeline {

    agent any

    options {
        skipDefaultCheckout(true)
        timestamps()
    }

    environment {
        COMPOSE_PROJECT_NAME = 'proyect-bigdata'
    }

    stages {

        stage('Checkout') {
            steps {
                echo '1. Descargando código desde GitHub...'
                checkout scm
            }
        }

        stage('Validar entorno') {
            steps {
                echo '2. Validando Docker Compose...'

                sh '''
                    docker --version
                    docker compose version
                    docker compose config --services
                '''
            }
        }

        stage('Build Dask') {
            steps {
                echo '3. Construyendo imagen Dask...'

                sh '''
                    docker compose build dask-client
                '''
            }
        }

        stage('Levantar infraestructura Dask') {
            steps {
                echo '4. Levantando MongoDB y cluster Dask...'

                sh '''
                    docker compose up -d \
                        mongo \
                        dask-scheduler \
                        dask-worker-1 \
                        dask-worker-2
                '''
            }
        }

        stage('Descargar dataset Kaggle') {

            steps {

                echo '5. Descargando dataset mediante credenciales Jenkins...'

                withCredentials([
                    usernamePassword(
                        credentialsId: 'kaggle-api',
                        usernameVariable: 'KAGGLE_USERNAME',
                        passwordVariable: 'KAGGLE_KEY'
                    )
                ]) {

                    sh '''
                        docker compose run --rm \
                            -e KAGGLE_USERNAME \
                            -e KAGGLE_KEY \
                            dask-client \
                            python scripts/download_dataset.py
                    '''
                }
            }
        }

        stage('Verificar dataset') {
            steps {

                echo '6. Comprobando dataset dentro del volumen Docker...'

                sh '''
                    docker compose run --rm --no-deps \
                        dask-client \
                        sh -lc "ls -lh /app/data/raw/"
                '''
            }
        }
    }

    post {

        success {
            echo '========================================'
            echo 'KAGGLE + JENKINS + DOCKER: OK'
            echo 'Dataset disponible automáticamente'
            echo '========================================'
        }

        failure {
            echo '========================================'
            echo 'PIPELINE FALLÓ'
            echo 'Revisar Console Output'
            echo '========================================'
        }
    }
}