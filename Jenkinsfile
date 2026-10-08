pipeline {

    agent any

    stages {

        stage('Checkout') {
            steps {
                echo 'Descargando código desde GitHub...'
                checkout scm
            }
        }

        stage('Verificar entorno') {
            steps {
                echo 'Verificando Git...'
                sh 'git --version'

                echo 'Verificando Docker...'
                sh 'docker --version'

                echo 'Verificando Docker Compose...'
                sh 'docker compose version'
            }
        }

        stage('Verificar archivos') {
            steps {
                echo 'Contenido del repositorio:'
                sh 'ls -la'

                echo 'Validando docker-compose.yml...'
                sh 'docker compose config --services'
            }
        }
    }

    post {

        success {
            echo 'PIPELINE DE PRUEBA EXITOSO'
        }

        failure {
            echo 'PIPELINE DE PRUEBA FALLÓ'
        }
    }
}