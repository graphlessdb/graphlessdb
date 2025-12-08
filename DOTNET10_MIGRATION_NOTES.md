# .NET 10 Migration Notes

## Overview
This document describes the changes made to upgrade GraphlessDB from .NET 8 to .NET 10, and the steps required to complete the migration.

## Changes Completed

### 1. SDK Version Update
- **File**: `global.json`
- **Change**: Updated SDK version from `8.0.101` to `10.0.100`
- **Status**: Complete

### 2. Target Framework Updates
Updated all project files to target `net10.0`:

- **GraphlessDB.csproj**: `net8.0` → `net10.0`
- **GraphlessDB.DynamoDB.csproj**: `net8.0` → `net10.0`
- **GraphlessDB.Tests.csproj**: `net8.0` → `net10.0`
- **GraphlessDB.DynamoDB.Tests.csproj**: `net8.0` → `net10.0`
- **GraphlessDB.Analyzers.csproj**: Remains on `netstandard2.0` (required for Roslyn analyzers)

**Status**: Complete

### 3. NuGet Package Updates

#### Core Libraries (GraphlessDB)
- `System.Text.Json`: `9.0.3` → `10.0.0`
- `System.Collections.Immutable`: `9.0.3` → `10.0.0`
- `Microsoft.Extensions.Options`: `9.0.3` → `10.0.0`
- `Microsoft.Extensions.Logging.Abstractions`: `9.0.3` → `10.0.0`

#### Test Libraries (Both Test Projects)
- `Microsoft.NET.Test.Sdk`: `17.11.1` → `17.12.0`
- `MSTest.TestAdapter`: `3.6.0` → `3.7.0`
- `MSTest.TestFramework`: `3.6.0` → `3.7.0`
- `Microsoft.Extensions.DependencyInjection`: `9.0.3` → `10.0.0`
- `AWSSDK.Extensions.NETCore.Setup`: `3.7.400` (unchanged)
- `coverlet.collector`: `6.0.2` (unchanged)

#### DynamoDB Library
- `AWSSDK.DynamoDBv2`: `3.7.406.15` → `3.7.407.0`

#### Analyzer Libraries
- `Microsoft.CodeAnalysis.CSharp`: `4.11.0` → `4.12.0`
- `Microsoft.CodeAnalysis.Analyzers`: `3.11.0` (unchanged)

**Status**: Complete (pending verification)

## Remaining Tasks

### 1. Install .NET 10 SDK
The build environment requires .NET SDK 10.0.100 to be installed:
- **Download**: https://dotnet.microsoft.com/en-us/download/dotnet/10.0
- **Current SDK**: 8.0.416

### 2. Verify Package Versions
Once .NET 10 SDK is installed, run the following commands to verify and update package versions:

```bash
# Check for outdated packages
dotnet list src/GraphlessDB.sln package --outdated

# Update all packages to latest compatible versions
dotnet list src/GraphlessDB.sln package --outdated | \
  grep ">" | \
  awk '{print $2}' | \
  xargs -I {} dotnet add package {}
```

### 3. Build Verification
Build the solution to identify any compilation errors:

```bash
cd /tmp/claude/graphlessdb-issue-386
export MSBUILDDISABLENODEREUSE=1
dotnet clean src/GraphlessDB.sln --nodereuse:false
dotnet build src/GraphlessDB.sln --nodereuse:false
```

Expected issues to address:
- API breaking changes in .NET 10
- Obsolete API usage
- New analyzer warnings

### 4. Test Verification
Run the full test suite to ensure compatibility:

```bash
cd /tmp/claude/graphlessdb-issue-386
export MSBUILDDISABLENODEREUSE=1
dotnet test src/GraphlessDB.sln --nodereuse:false
```

Verify:
- All tests pass
- No new test failures
- Performance remains acceptable

### 5. CI/CD Pipeline Updates
Update CI/CD configuration to use .NET 10 SDK:
- Update GitHub Actions workflow files
- Update Docker base images to .NET 10 runtime
- Update documentation with new SDK requirements

### 6. Documentation Updates
Update project documentation:
- README.md: Update prerequisites to .NET 10
- Contributing guide: Update development environment setup
- Build instructions: Update SDK version requirements

## Breaking Changes in .NET 10

Review the official breaking changes documentation:
https://learn.microsoft.com/en-us/dotnet/core/compatibility/10.0

Key areas to review:
- Core .NET libraries
- ASP.NET Core (if applicable)
- Entity Framework Core (if applicable)
- MSBuild and SDK changes

## Package Version Notes

### Microsoft.Extensions.* Packages
The Microsoft.Extensions packages typically align with the .NET version. Version 10.0.0 is expected to be available with .NET 10 release.

### AWS SDK Packages
AWS SDK packages follow their own versioning and are generally compatible across .NET versions. The current versions should work with .NET 10, but verify:
- `AWSSDK.DynamoDBv2`
- `AWSSDK.Extensions.NETCore.Setup`

### Test Framework Packages
MSTest packages have been updated to version 3.7.0, which should support .NET 10. Verify compatibility after installation.

### Roslyn Analyzer Packages
The GraphlessDB.Analyzers project remains on netstandard2.0 as required by Roslyn. Updated Microsoft.CodeAnalysis.CSharp to 4.12.0 for C# 13 support that comes with .NET 10.

## Rollback Plan

If issues arise during migration:

1. Revert global.json:
   ```json
   {
     "sdk": {
       "version": "8.0.101",
       "rollForward": "latestFeature"
     }
   }
   ```

2. Revert all TargetFramework changes:
   - Change `<TargetFramework>net10.0</TargetFramework>` back to `<TargetFramework>net8.0</TargetFramework>`

3. Revert package version updates

## Next Steps

1. Install .NET 10 SDK on development and CI/CD environments
2. Run build and tests to verify compatibility
3. Address any compilation errors or test failures
4. Update CI/CD pipelines
5. Update documentation
6. Create pull request with all changes
7. Conduct code review focusing on .NET 10 compatibility
8. Merge after approval and successful CI/CD runs

## Support and Resources

- .NET 10 Documentation: https://learn.microsoft.com/en-us/dotnet/core/whats-new/dotnet-10/overview
- .NET 10 Breaking Changes: https://learn.microsoft.com/en-us/dotnet/core/compatibility/10.0
- .NET 10 Download: https://dotnet.microsoft.com/en-us/download/dotnet/10.0
- Migration Guide: https://learn.microsoft.com/en-us/dotnet/core/migration/

## Contact

For questions or issues during migration, contact the development team or create a GitHub issue.
