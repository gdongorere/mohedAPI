using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using Serilog;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Services.Auth;
using Eswatini.Health.Api.Services.Encryption;
using Eswatini.Health.Api.Services.Indicators;
using Eswatini.Health.Api.Services.Period;
using Eswatini.Health.Api.Services.Targets;
using Eswatini.Health.Api.Services.ETL;

namespace Eswatini.Health.Api.Startup;

public static class ServiceExtensions
{
    public static IServiceCollection AddDatabaseContexts(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddDbContext<StagingDbContext>(options =>
            options.UseSqlServer(
                configuration.GetConnectionString("StagingConnection"),
                sqlServer => sqlServer.CommandTimeout(60)
                    .EnableRetryOnFailure(3)
            ));

        return services;
    }

    public static IServiceCollection AddApplicationServices(this IServiceCollection services)
    {
        // Core services
        services.AddScoped<IJwtService, JwtService>();
        services.AddScoped<IEncryptionService, EncryptionService>();
        services.AddScoped<IPeriodService, PeriodService>();

        // Indicator services
        services.AddScoped<IHIVIndicatorService, HIVIndicatorService>();
        services.AddScoped<IPreventionIndicatorService, PreventionIndicatorService>();
        
        // Target services
        services.AddScoped<ITargetService, TargetService>();

        // ETL services
services.AddScoped<IHTSETLService, HTSETLService>();
services.AddScoped<IPrEPETLService, PrEPETLService>();
services.AddScoped<IARTETLService, ARTETLService>();
services.AddScoped<IETLService, ETLService>();

        return services;
    }

    public static IServiceCollection AddJwtAuthentication(this IServiceCollection services, IConfiguration configuration)
    {
        var jwtKey = configuration["Jwt:Key"] ??
            throw new InvalidOperationException("JWT Key not configured");

        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey)),
                    ValidateIssuer = true,
                    ValidIssuer = configuration["Jwt:Issuer"],
                    ValidateAudience = true,
                    ValidAudience = configuration["Jwt:Audience"],
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.Zero
                };

                options.Events = new JwtBearerEvents
                {
                    OnAuthenticationFailed = context =>
                    {
                        Log.Warning("Authentication failed: {Error}", context.Exception.Message);
                        return Task.CompletedTask;
                    }
                };
            });

        return services;
    }

    public static IServiceCollection AddAuthorizationPolicies(this IServiceCollection services)
    {
        services.AddAuthorization(options =>
        {
            options.AddPolicy("AdminOnly", policy => policy.RequireRole("admin"));
            options.AddPolicy("Viewer", policy => policy.RequireRole("admin", "viewer"));
        });

        return services;
    }

    public static IServiceCollection AddCorsPolicies(this IServiceCollection services)
    {
        services.AddCors(options =>
        {
            options.AddPolicy("AngularApp", policy =>
            {
                policy.WithOrigins("http://localhost:4200", "https://yourdomain.com")
                      .AllowAnyHeader()
                      .AllowAnyMethod()
                      .AllowCredentials();
            });
        });

        return services;
    }

    public static IServiceCollection AddSwaggerWithJwt(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo
            {
                Title = "Eswatini Health API",
                Version = "v1",
                Description = "Health Indicators API for Executive Dashboard"
            });

            c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
            {
                Description = "JWT Authorization header using the Bearer scheme",
                Name = "Authorization",
                In = ParameterLocation.Header,
                Type = SecuritySchemeType.Http,
                Scheme = "bearer"
            });

            c.AddSecurityRequirement(new OpenApiSecurityRequirement
            {
                {
                    new OpenApiSecurityScheme
                    {
                        Reference = new OpenApiReference
                        {
                            Type = ReferenceType.SecurityScheme,
                            Id = "Bearer"
                        }
                    },
                    new string[] {}
                }
            });
        });

        return services;
    }
}