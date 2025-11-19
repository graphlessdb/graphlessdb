rm -r ./bin/test/results
dotnet test --collect:"XPlat Code Coverage" --settings:"settings.runsettings" --filter:"TestCategory!=Integration" ./GraphlessDB.sln
reportgenerator -reports:"./bin/test/results/**/coverage.cobertura.xml" -targetdir:"./bin/test/results/codecoverage/"