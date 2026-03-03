namespace Eswatini.Health.Api.Common.Responses;

public static class DevelopmentResponses
{
    public static IResult EndpointInDevelopment(string endpointName)
    {
        return Results.Ok(new
        {
            success = true,
            timestamp = DateTime.UtcNow,
            message = $"The '{endpointName}' endpoint is in development. Real data will be available once the required data sources are available.",
            data = new
            {
                status = "in_development",
                expectedData = GetExpectedDataDescription(endpointName)
            }
        });
    }
    
    private static string GetExpectedDataDescription(string endpointName)
    {
        return endpointName switch
        {
            "dashboard" => "Dashboard will show: Total on ART, VL Tested, VL Suppressed, HIV Testing, PrEP Initiations, and calculated rates",
            "indicators" => "List of all available indicators with their codes and names",
            "indicator_data" => "Historical indicator data with periods, values, and trends",
            "facilities" => "List of all facilities with their metrics",
            "regions" => "List of regions",
            "art" => "ART outcomes including TX_CURR, TX_NEW, and viral load data",
            "tb" => "TB indicators including screening, diagnosis, and treatment data",
            _ => "Real data coming soon"
        };
    }
}