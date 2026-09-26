using EcomAPI.Models;
using EcomAPI.Outbox;
using Microsoft.EntityFrameworkCore;

namespace EcomAPI.Data
{
    public class WriteDbContext : DbContext
    {
        public WriteDbContext(DbContextOptions<WriteDbContext> options) : base(options)
        {
            
        }

        public DbSet<Order> Orders { get; set; } = null!;

        public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OutboxMessage>().HasIndex(m => m.ProcessedAt);
        }
        
    }
}
