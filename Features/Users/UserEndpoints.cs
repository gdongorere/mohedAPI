using Microsoft.EntityFrameworkCore;
using System.Security.Claims;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Auth;

namespace Eswatini.Health.Api.Features.Users;

public static class UserEndpoints
{
    public static IEndpointRouteBuilder MapUserEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/users");

        group.MapGet("/me", GetCurrentUser)
             .WithName("GetCurrentUser")
             .WithOpenApi();

        group.MapGet("/", GetAllUsers)
             .WithName("GetAllUsers")
             .WithOpenApi()
             .RequireAuthorization("AdminOnly");

        return app;
    }

    private static async Task<IResult> GetCurrentUser(
        StagingDbContext db,
        ClaimsPrincipal user)
    {
        var userId = user.FindFirst(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub)?.Value;
        
        if (string.IsNullOrEmpty(userId))
            return Results.Unauthorized();

        var dbUser = await db.Users.FindAsync(userId);
        
        if (dbUser == null)
            return Results.NotFound();

        return Results.Ok(new
        {
            success = true,
            data = new UserDto
            {
                Id = dbUser.Id,
                Email = dbUser.Email,
                Name = dbUser.Name,
                Surname = dbUser.Surname,
                Role = dbUser.Role,
                IsActive = dbUser.IsActive
            }
        });
    }

    private static async Task<IResult> GetAllUsers(
        StagingDbContext db)
    {
        var users = await db.Users
            .Select(u => new UserDto
            {
                Id = u.Id,
                Email = u.Email,
                Name = u.Name,
                Surname = u.Surname,
                Role = u.Role,
                IsActive = u.IsActive
            })
            .ToListAsync();

        return Results.Ok(new
        {
            success = true,
            count = users.Count,
            data = users
        });
    }
}