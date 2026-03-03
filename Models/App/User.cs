using Eswatini.Health.Api.Models.Common;

namespace Eswatini.Health.Api.Models.App;

public class User : EntityBase
{
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Surname { get; set; } = string.Empty;  // Make nullable!
    public string Role { get; set; } = "viewer";  // admin, viewer
    public string PasswordHash { get; set; } = string.Empty;
    public DateTime? LastLoginAt { get; set; }
    public bool IsActive { get; set; } = true;
}