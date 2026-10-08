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
                echo '2. Validando Docker y Compose...'

                sh '''
                    docker --version
                    docker compose version
                    docker compose config --services
                '''
            }
        }

        stage('Build imágenes') {
            steps {
                echo '3. Construyendo imágenes Docker...'

                sh '''
                    docker compose build \
                        dask-client \
                        dask-scheduler \
                        dask-worker-1 \
                        dask-worker-2 \
                        spark-master \
                        spark-worker \
                        api
                '''
            }
        }

        stage('Levantar Mongo y Dask') {
            steps {
                echo '4. Levantando MongoDB y cluster Dask...'

                sh '''
                    docker compose up -d \
                        mongo \
                        dask-scheduler \
                        dask-worker-1 \
                        dask-worker-2
                '''

                sleep 10
            }
        }

        stage('Descargar dataset') {
            steps {

                echo '5. Descargando dataset desde Kaggle...'

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

        stage('Ingesta Dask') {
            steps {
                echo '6. Ejecutando limpieza e ingesta distribuida con Dask...'

                sh '''
                    docker compose run --rm \
                        dask-client \
                        python dask/ingest.py
                '''
            }
        }

        stage('Verificar MongoDB') {
            steps {
                echo '7. Verificando registros cargados en MongoDB...'

                sh '''
                    COUNT=$(docker exec bigdata-mongo \
                        mongosh crime_db \
                        --quiet \
                        --eval "db.crimes.countDocuments()")

                    echo "Registros MongoDB: $COUNT"

                    test "$COUNT" -gt 1000000
                '''
            }
        }

        stage('Levantar Spark') {
            steps {
                echo '8. Levantando Spark Master y Worker...'

                sh '''
                    docker compose up -d \
                        spark-master \
                        spark-worker
                '''

                sleep 10
            }
        }

        stage('Procesamiento Spark') {
            steps {
                echo '9. Ejecutando agregaciones Spark...'

                sh '''
                    docker exec bigdata-spark-master sh -lc '
                    /opt/spark/bin/spark-submit \
                      --master spark://spark-master:7077 \
                      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 \
                      /app/spark/process.py
                    '
                '''
            }
        }

        stage('Verificar resultados Spark') {
            steps {
                echo '10. Verificando colecciones generadas por Spark...'

                sh '''
                    MONTHLY=$(docker exec bigdata-mongo \
                        mongosh crime_db \
                        --quiet \
                        --eval "db.crime_monthly_area_stats.countDocuments()")

                    HOTSPOTS=$(docker exec bigdata-mongo \
                        mongosh crime_db \
                        --quiet \
                        --eval "db.crime_hotspots_grid.countDocuments()")

                    echo "Agregaciones mensuales: $MONTHLY"
                    echo "Celdas hotspots: $HOTSPOTS"

                    test "$MONTHLY" -gt 0
                    test "$HOTSPOTS" -gt 0
                '''
            }
        }

        stage('Deploy API') {
            steps {
                echo '11. Desplegando API Flask...'

                sh '''
                    docker compose up -d api
                '''

                sleep 5
            }
        }

        stage('Pytest') {
            steps {
                echo '12. Ejecutando pruebas automatizadas...'

                sh '''
                    docker compose exec -T api \
                        pytest -v tests/test_api.py
                '''
            }
        }

        stage('Smoke test') {
            steps {
                echo '13. Verificando API desplegada...'

                sh '''
                    for i in 1 2 3 4 5 6 7 8 9 10
                    do
                        if curl --fail --silent \
                            http://api:5000/health
                        then
                            echo
                            echo "API disponible correctamente"
                            exit 0
                        fi

                        echo "Esperando API..."
                        sleep 3
                    done

                    echo "ERROR: API no respondió"
                    exit 1
                '''
            }
        }
    }

    post {

        success {
            echo '========================================'
            echo '        PIPELINE CI/CD EXITOSO'
            echo '========================================'
            echo 'Kaggle  : OK'
            echo 'Dask    : OK'
            echo 'MongoDB : OK'
            echo 'Spark   : OK'
            echo 'Pytest  : OK'
            echo 'Flask   : DEPLOYED'
            echo '========================================'
        }

        failure {
            echo '========================================'
            echo 'PIPELINE CI/CD FALLÓ'
            echo 'El despliegue no se considera válido.'
            echo 'Revisar la etapa que produjo el error.'
            echo '========================================'
        }
    }
}