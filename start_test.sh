currentPath=$(pwd)
relativePathToScript="./java-code/my-project"

cd ${relativePathToScript}
bash build-java.sh
cd ${currentPath}
