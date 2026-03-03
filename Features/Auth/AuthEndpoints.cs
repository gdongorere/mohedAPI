using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.App;
using Eswatini.Health.Api.Models.DTOs.Auth;
using Eswatini.Health.Api.Services.Auth;
using Eswatini.Health.Api.Services.Encryption;
using System.Security.Claims;

namespace Eswatini.Health.Api.Features.Auth;

public static class AuthEndpoints
{
    public static IEndpointRouteBuilder MapAuthEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/auth");

        group.MapPost("/login", Login)
             .WithName("Login")
             .WithOpenApi()
             .AllowAnonymous();

        group.MapPost("/register", Register)
             .WithName("Register")
             .WithOpenApi()
             .AllowAnonymous();

        return app;
    }

    private static async Task<IResult> Login(
        LoginRequest request,
        StagingDbContext db,
        IJwtService jwt,
        IEncryptionService encryption)
    {
        try
        {
            if (string.IsNullOrEmpty(request.Email) || string.IsNullOrEmpty(request.Password))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Email and password are required"
                });
            }

            var user = await db.Users.FirstOrDefaultAsync(u => u.Email == request.Email && u.IsActive);

            if (user == null || !encryption.VerifyPassword(request.Password, user.PasswordHash))
            {
                return Results.Json(new
                {
                    success = false,
                    message = "Invalid email or password"
                }, statusCode: 401);
            }

            user.LastLoginAt = DateTime.UtcNow;
            await db.SaveChangesAsync();

            var token = jwt.GenerateToken(user);

            var response = new LoginResponse
            {
                Token = token,
                ExpiresIn = 86400,
                User = new UserDto
                {
                    Id = user.Id,
                    Email = user.Email,
                    Name = user.Name,
                    Surname = user.Surname,
                    Role = user.Role,
                    IsActive = user.IsActive
                }
            };

            return Results.Ok(new
            {
                success = true,
                message = "Login successful",
                data = response
            });
        }
        catch (Exception ex)
        {
            return Results.Problem("An error occurred during login: " + ex.Message, statusCode: 500);
        }
    }

    private static async Task<IResult> Register(
        RegisterRequest request,
        StagingDbContext db,
        IEncryptionService encryption)
    {
        try
        {
            if (string.IsNullOrEmpty(request.Email) || 
                string.IsNullOrEmpty(request.Password) || 
                string.IsNullOrEmpty(request.Name))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Email, name, and password are required"
                });
            }

            if (!IsValidEmail(request.Email))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Invalid email format"
                });
            }

            if (request.Password.Length < 8)
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Password must be at least 8 characters long"
                });
            }

            var existingUser = await db.Users.FirstOrDefaultAsync(u => u.Email == request.Email);
            if (existingUser != null)
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "User with this email already exists"
                });
            }

            var user = new User
            {
                Id = Guid.NewGuid().ToString(),
                Email = request.Email.ToLower().Trim(),
                Name = request.Name.Trim(),
                Surname = request.Surname?.Trim() ?? string.Empty,
                Role = "viewer",
                PasswordHash = encryption.HashPassword(request.Password),
                IsActive = true
            };

            await db.Users.AddAsync(user);
            await db.SaveChangesAsync();

            return Results.Ok(new
            {
                success = true,
                message = "Registration successful. You can now login.",
                data = new
                {
                    user.Id,
                    user.Email,
                    user.Name,
                    user.Surname,
                    user.Role,
                    user.IsActive
                }
            });
        }
        catch (Exception ex)
        {
            return Results.Problem("An error occurred during registration: " + ex.Message, statusCode: 500);
        }
    }

    private static bool IsValidEmail(string email)
    {
        try
        {
            var addr = new System.Net.Mail.MailAddress(email);
            return addr.Address == email;
        }
        catch
        {
            return false;
        }
    }
}