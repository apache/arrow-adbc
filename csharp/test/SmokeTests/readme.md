# Smoke Tests
The smoke tests are designed to work with the NuGet packages that are generated from the `dotnet pack` command. These are run automatically as part of the `Build-All.ps1` script.

## Smoke Test Structure
Smoke tests are similar to the unit tests, and actually leverage the code from unit tests, but instead target using the NuGet packages (instead of the project references) to validate they function correctly.

Since these are smoke tests, they run a scaled down version of what the unit tests run. To do this, each `*.Tests` project has an equivalent `*.SmokeTests` project. For example, there is both `Apache.Arrow.Adbc.Tests` and `Apache.Arrow.Adbc.SmokeTests`. The smoke tests use linking to add items from the `*.Test` project and make them appear to be included, but they are actually located in the original project.

The smoke tests only run the ClientTests because those exercise multiple NuGet packages and the underlying driver.
