namespace Eswatini.Health.Api.Common.Constants;

public static class ApiRoutes
{
    public const string Health = "/health";
    public const string BaseApi = "/api";

    public static class Auth
    {
        private const string Base = "/api/auth";
        public const string Login = Base + "/login";
        public const string Register = Base + "/register";
        public const string Me = Base + "/me";
    }

    public static class Indicators
    {
        private const string Base = "/api/indicators";
        public const string GetAll = Base;
        public const string GetData = Base + "/data";
        public const string GetTrends = Base + "/trends";
        public const string GetBreakdown = Base + "/breakdown";
    }

    public static class Dashboard
    {
        public const string Get = "/api/dashboard";
        public const string GetSummary = "/api/dashboard/summary";
        public const string GetHIV = "/api/dashboard/hiv";
        public const string GetPrevention = "/api/dashboard/prevention";
    }

    public static class Regions
    {
        public const string GetAll = "/api/regions";
    }

    public static class Targets
    {
        private const string Base = "/api/targets";
        public const string GetAll = Base;
        public const string Get = Base + "/{id}";
        public const string Create = Base;
        public const string Update = Base + "/{id}";
        public const string Delete = Base + "/{id}";
    }

    public static class Users
    {
        private const string Base = "/api/users";
        public const string GetCurrent = Base + "/me";
    }
}