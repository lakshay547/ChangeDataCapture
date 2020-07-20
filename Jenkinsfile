pipeline{

    agent any

    stages{

        tools{

            maven 'Maven'
        }

        stage("build"){

            steps{
                echo "Building the Application"
                sh "mvn compile"
            }
        }

        stage("Test"){

            steps{
                echo "Testing the Application"
            }
        }

        stage("Deploy"){

            steps{
                echo "Deploying the Application"
            }
        }
    }
}