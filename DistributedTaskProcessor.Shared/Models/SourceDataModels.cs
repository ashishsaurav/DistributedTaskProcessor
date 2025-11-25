using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DistributedTaskProcessor.Shared.Models;

/// <summary>
/// Source market data from external database
/// </summary>
[Table("SourceMarketData")]
public class SourceMarketData
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(50)]
    public string Symbol { get; set; } = string.Empty;

    [Required]
    [MaxLength(50)]
    public string Fund { get; set; } = string.Empty;

    public DateTime RunDate { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal Price { get; set; }

    public long Volume { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? OpenPrice { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? ClosePrice { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? HighPrice { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? LowPrice { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Calculated results stored in destination database
/// </summary>
[Table("CalculatedResults")]
public class CalculatedResult
{
    [Key]
    public long Id { get; set; }

    public Guid TaskId { get; set; }

    public long SourceId { get; set; }

    [Required]
    [MaxLength(50)]
    public string Symbol { get; set; } = string.Empty;

    [Required]
    [MaxLength(50)]
    public string Fund { get; set; } = string.Empty;

    public DateTime RunDate { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal Price { get; set; }

    public long Volume { get; set; }

    // Calculated fields
    [Column(TypeName = "decimal(18,4)")]
    public decimal? MovingAverage { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? VolumeWeightedPrice { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? PriceChange { get; set; }

    [Column(TypeName = "decimal(18,4)")]
    public decimal? VolatilityIndex { get; set; }

    [MaxLength(20)]
    public string? TrendIndicator { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Result message sent to Kafka result queue
/// </summary>
public class ProcessedTaskResult
{
    public Guid TaskId { get; set; }
    public List<CalculatedResult> Results { get; set; } = new();
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string WorkerId { get; set; } = string.Empty;
    public int ProcessedRows { get; set; }
    public DateTime CompletedAt { get; set; } = DateTime.UtcNow;
}
