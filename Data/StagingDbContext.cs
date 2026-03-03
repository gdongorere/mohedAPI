using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Models.App;
using Eswatini.Health.Api.Models.Staging;
using Eswatini.Health.Api.Models.Targets;

namespace Eswatini.Health.Api.Data;

public class StagingDbContext : DbContext
{
    public StagingDbContext(DbContextOptions<StagingDbContext> options) : base(options) { }

    // User Management
    public DbSet<User> Users { get; set; }

    // Core Data Tables (matching your SQL scripts)
    public DbSet<IndicatorValueHIV> IndicatorValues_HIV { get; set; }
    public DbSet<IndicatorValuePrevention> IndicatorValues_Prevention { get; set; }
    public DbSet<IndicatorValueTB> IndicatorValues_TB { get; set; }
    
    // Targets
    public DbSet<IndicatorTarget> IndicatorTargets { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Users
        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.Email).IsUnique();
            entity.Property(e => e.Email).HasMaxLength(255).IsRequired();
            entity.Property(e => e.Name).HasMaxLength(255).IsRequired();
            entity.Property(e => e.Surname).HasMaxLength(255);
            entity.Property(e => e.Role).HasMaxLength(50).HasDefaultValue("viewer");
            entity.Property(e => e.PasswordHash).HasMaxLength(500).IsRequired();
            entity.Property(e => e.IsActive).HasDefaultValue(true);
        });

        // HIV Indicators
        modelBuilder.Entity<IndicatorValueHIV>(entity =>
        {
            entity.ToTable("IndicatorValues_HIV");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Indicator).HasMaxLength(50).IsRequired();
            entity.Property(e => e.RegionId).IsRequired();
            entity.Property(e => e.VisitDate).IsRequired();
            entity.Property(e => e.AgeGroup).HasMaxLength(20).IsRequired();
            entity.Property(e => e.Sex).HasMaxLength(10).IsRequired();
            entity.Property(e => e.PopulationType).HasMaxLength(50);
            entity.Property(e => e.Value).IsRequired();
            
            entity.HasIndex(e => new { e.Indicator, e.RegionId, e.VisitDate, e.AgeGroup, e.Sex, e.PopulationType })
                .HasDatabaseName("IX_HIV_Lookup");
            entity.HasIndex(e => e.VisitDate).HasDatabaseName("IX_HIV_DateRange");
            entity.HasIndex(e => e.UpdatedAt).HasDatabaseName("IX_HIV_Updated");
        });

        // Prevention Indicators
        modelBuilder.Entity<IndicatorValuePrevention>(entity =>
        {
            entity.ToTable("IndicatorValues_Prevention");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Indicator).HasMaxLength(50).IsRequired();
            entity.Property(e => e.RegionId).IsRequired();
            entity.Property(e => e.VisitDate).IsRequired();
            entity.Property(e => e.AgeGroup).HasMaxLength(20).IsRequired();
            entity.Property(e => e.Sex).HasMaxLength(10).IsRequired();
            entity.Property(e => e.PopulationType).HasMaxLength(50);
            entity.Property(e => e.Value).IsRequired();
            
            entity.HasIndex(e => new { e.Indicator, e.RegionId, e.VisitDate, e.AgeGroup, e.Sex, e.PopulationType })
                .HasDatabaseName("IX_Prevention_Lookup");
            entity.HasIndex(e => e.VisitDate).HasDatabaseName("IX_Prevention_DateRange");
            entity.HasIndex(e => e.UpdatedAt).HasDatabaseName("IX_Prevention_Updated");
        });

        // TB Indicators
        modelBuilder.Entity<IndicatorValueTB>(entity =>
        {
            entity.ToTable("IndicatorValues_TB");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Indicator).HasMaxLength(50).IsRequired();
            entity.Property(e => e.RegionId).IsRequired();
            entity.Property(e => e.VisitDate).IsRequired();
            entity.Property(e => e.AgeGroup).HasMaxLength(20).IsRequired();
            entity.Property(e => e.Sex).HasMaxLength(10).IsRequired();
            entity.Property(e => e.PopulationType).HasMaxLength(50);
            entity.Property(e => e.TBType).HasMaxLength(50);
            entity.Property(e => e.Value).IsRequired();
            
            entity.HasIndex(e => new { e.Indicator, e.RegionId, e.VisitDate, e.AgeGroup, e.Sex, e.TBType, e.PopulationType })
                .HasDatabaseName("IX_TB_Lookup");
            entity.HasIndex(e => e.VisitDate).HasDatabaseName("IX_TB_DateRange");
            entity.HasIndex(e => e.UpdatedAt).HasDatabaseName("IX_TB_Updated");
        });

        // Targets
        modelBuilder.Entity<IndicatorTarget>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Indicator).HasMaxLength(50).IsRequired();
            entity.Property(e => e.RegionId).IsRequired();
            entity.Property(e => e.Year).IsRequired();
            entity.Property(e => e.TargetValue).HasPrecision(18, 2).IsRequired();
            entity.Property(e => e.TargetType).HasMaxLength(20).HasDefaultValue("number");
            entity.Property(e => e.Notes).HasMaxLength(500);
            entity.Property(e => e.CreatedBy).HasMaxLength(36).IsRequired();
            entity.Property(e => e.AgeGroup).HasMaxLength(20);
            entity.Property(e => e.Sex).HasMaxLength(10);
            entity.Property(e => e.PopulationType).HasMaxLength(50);
            
            entity.HasIndex(e => new { e.Indicator, e.RegionId, e.Year, e.Quarter, e.Month })
                .HasDatabaseName("IX_Targets_Lookup");
        });
    }
}