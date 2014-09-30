To deploy:
mvn clean verify gpg:sign install:install deploy:deploy -Phadoop-1 -DskipTests -Psources -Pjavadoc -Dgpg.passphrase=XXX

