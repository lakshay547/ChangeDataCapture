pipeline{

    agent any

    stages{

        stage("build"){

            steps{
                echo "Building the Application"
                mvn compile
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