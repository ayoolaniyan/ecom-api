# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Restore first so the NuGet layer is cached until the project file changes
COPY EcomAPI.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o /app/publish --no-restore /p:UseAppHost=false

# ---- Runtime stage ----
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# SQLite databases live here; mount a volume to persist them
RUN mkdir -p /app/data && chown app:app /app/data

COPY --from=build /app/publish ./

ENV ASPNETCORE_HTTP_PORTS=8080 \
    ConnectionStrings__WriteDbConnection="Data Source=/app/data/Write.db" \
    ConnectionStrings__ReadDbConnection="Data Source=/app/data/Read.db"

USER app
EXPOSE 8080

ENTRYPOINT ["dotnet", "EcomAPI.dll"]
