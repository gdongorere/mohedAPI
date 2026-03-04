using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;
using Eswatini.Health.Api.Services.Targets;
using Eswatini.Health.Api.Models.DTOs.Targets;

namespace Eswatini.Health.Api.Features.Targets;

public static class TargetEndpoints
{
    public static IEndpointRouteBuilder MapTargetEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/targets");

        // GET endpoints - Admin only (as requested)
        group.MapGet("/", GetTargets)
            .WithName("GetTargets")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapGet("/{id}", GetTarget)
            .WithName("GetTarget")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapGet("/summary/{indicator}", GetTargetSummary)
            .WithName("GetTargetSummary")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        // POST/PUT/DELETE endpoints - Admin only
        group.MapPost("/", CreateTarget)
            .WithName("CreateTarget")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapPut("/{id}", UpdateTarget)
            .WithName("UpdateTarget")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapDelete("/{id}", DeleteTarget)
            .WithName("DeleteTarget")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        return app;
    }

    private static async Task<IResult> GetTargets(
        [FromServices] ITargetService targetService,
        [FromQuery] string? indicator = null,
        [FromQuery] int? regionId = null,
        [FromQuery] int? year = null)
    {
        try
        {
            var targets = await targetService.GetTargetsAsync(indicator, regionId, year);
            
            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                count = targets.Count,
                data = targets
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = new List<object>()
            });
        }
    }

    private static async Task<IResult> GetTarget(
        int id,
        [FromServices] ITargetService targetService)
    {
        try
        {
            var targets = await targetService.GetTargetsAsync();
            var target = targets.FirstOrDefault(t => t.Id == id);
            
            if (target == null)
            {
                return Results.Ok(new
                {
                    success = false,
                    message = "Target not found",
                    timestamp = DateTime.UtcNow,
                    data = (object?)null
                });
            }

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = target
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
    }

    private static async Task<IResult> GetTargetSummary(
        string indicator,
        [FromServices] ITargetService targetService,
        [FromQuery] int year,
        [FromQuery] int? quarter = null,
        [FromQuery] int? month = null)
    {
        try
        {
            var summary = await targetService.GetTargetSummaryAsync(indicator, year, quarter, month);
            
            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = summary
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
    }

    private static async Task<IResult> CreateTarget(
        CreateTargetRequest request,
        [FromServices] ITargetService targetService,
        ClaimsPrincipal user)
    {
        try
        {
            var userId = user.FindFirst(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub)?.Value;
            
            if (string.IsNullOrEmpty(userId))
            {
                return Results.Ok(new
                {
                    success = false,
                    message = "User not authenticated",
                    timestamp = DateTime.UtcNow,
                    data = (object?)null
                });
            }

            var target = await targetService.CreateTargetAsync(request, userId);
            
            return Results.Ok(new
            {
                success = true,
                message = "Target created successfully",
                timestamp = DateTime.UtcNow,
                data = target
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
    }

    private static async Task<IResult> UpdateTarget(
        int id,
        UpdateTargetRequest request,
        [FromServices] ITargetService targetService)
    {
        try
        {
            var target = await targetService.UpdateTargetAsync(id, request);
            
            if (target == null)
            {
                return Results.Ok(new
                {
                    success = false,
                    message = "Target not found",
                    timestamp = DateTime.UtcNow,
                    data = (object?)null
                });
            }

            return Results.Ok(new
            {
                success = true,
                message = "Target updated successfully",
                timestamp = DateTime.UtcNow,
                data = target
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
    }

    private static async Task<IResult> DeleteTarget(
        int id,
        [FromServices] ITargetService targetService)
    {
        try
        {
            var result = await targetService.DeleteTargetAsync(id);
            
            if (!result)
            {
                return Results.Ok(new
                {
                    success = false,
                    message = "Target not found",
                    timestamp = DateTime.UtcNow,
                    data = (object?)null
                });
            }

            return Results.Ok(new
            {
                success = true,
                message = "Target deleted successfully",
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = (object?)null
            });
        }
    }
}