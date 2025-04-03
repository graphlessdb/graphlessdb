rm -r ./bin/test/results
dotnet test --collect:"XPlat Code Coverage" --settings:"settings.runsettings" --filter:"TestCategory!=Integration" ./GraphlessDB.sln
dotnet reportgenerator -reports:"./bin/test/results/**/coverage.cobertura.xml" -targetdir:"./bin/test/results/codecoverage/"