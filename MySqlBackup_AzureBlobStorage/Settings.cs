namespace MySqlBackup_AzureBlobStorage;

internal class Settings
{
	public string DatabaseConnectionString { get; init; }
	public string ContainerName { get; init; }
	public string StorageAccountConnectionString { get; init; }
	public int RetentionDays { get; init; }
}