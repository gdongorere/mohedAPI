namespace Eswatini.Health.Api.Services.Encryption;

public interface IEncryptionService
{
    string HashPassword(string password);
    bool VerifyPassword(string password, string hash);
}

public class EncryptionService : IEncryptionService
{
    private readonly ILogger<EncryptionService> _logger;

    public EncryptionService(ILogger<EncryptionService> logger)
    {
        _logger = logger;
    }

    public string HashPassword(string password)
    {
        return BCrypt.Net.BCrypt.HashPassword(password);
    }

    public bool VerifyPassword(string password, string hash)
    {
        return BCrypt.Net.BCrypt.Verify(password, hash);
    }
}